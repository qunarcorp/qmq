// yuzhaohui
// 12/5/2016
using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;

using NLog;
using System.Net;

namespace Qunar.TC.Qmq.Client.Transport
{
    /// <summary>
    /// TODO 批量优化
    /// </summary>
    internal class SocketChannel : Channel
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private const int READING = 1;

        private const int IDLE = 0;

        private readonly ConcurrentQueue<Holder> writeQueue;

        private volatile int reading = 0;

        private volatile int close = 0;

        private volatile byte writtable = 1;

        private volatile bool inFlush;

        //超过高水位线则不可写，低于低水位线重新可写
        private int waterMark;
        private const int LOW_WATER_MARK = 32 * 1024;
        private const int HIGH_WATER_MARK = 10 * 1024 * 1024;

        private const int LOH = 5 * 1024;
        private const int POOL_SIZE = 10;

        protected volatile Socket socket;

        private readonly ChannelPipeline pipeline;

        private readonly int readBufferSize;

        private readonly Pool _pool;

        private static readonly EventLoop EVENTLOOP = new EventLoop(8);

        private readonly SingleEventLoop _currentEventLoop;

        private readonly Action _receiveTask;
        private readonly Action _closeTask;
        private readonly Action _flushTask;

        private readonly SocketAsyncEventArgs writeArgs;
        private readonly SocketAsyncEventArgs receiveArgs;

        public SocketChannel(Socket socket, int readBufferSize)
        {
            this.socket = socket;
            this.readBufferSize = readBufferSize;
            this.writeQueue = new ConcurrentQueue<Holder>();
            this.pipeline = new ChannelPipeline(this);
            _currentEventLoop = EVENTLOOP.NextWorker();

            writeArgs = new SocketAsyncEventArgs();
            writeArgs.Completed += (sender, e) =>
            {
                ProcessWriteCompleted(true);
            };
            receiveArgs = new SocketAsyncEventArgs();
            receiveArgs.Completed += (sender, e) =>
            {
                ProcessReceiveCompleted();
            };

            //if allocate big buffer, then use pool
            if (readBufferSize > LOH)
            {
                _pool = new ByteBufferPool(readBufferSize, POOL_SIZE);
            }
            else
            {
                _pool = new MockPool(readBufferSize);
            }

            _receiveTask = () =>
            {
                if (close == 1) return;
                if (Interlocked.CompareExchange(ref reading, READING, IDLE) == IDLE)
                {
                    DoReceive();
                }
            };

            _closeTask = () =>
            {
                if (Interlocked.CompareExchange(ref close, 1, 0) != 0) return;

                Dequeue();
                try
                {
                    if (socket != null)
                    {
                        socket.Close();
                    }
                }
                catch (Exception e)
                {
                    logger.Warn(e, "close socket error");
                }
                finally
                {
                    TriggerInActive();
                }
            };

            _flushTask = () =>
            {
                Flush();
            };
        }

        public void Write(object msg)
        {
            if (!(msg is byte[]))
            {
                pipeline.Write(msg);
            }
            else
            {
                Write((byte[])msg);
            }
        }

        private void Write(byte[] buffer)
        {
            var p = new Packet(buffer, 0, buffer.Length);
            Enqueue(p);
            _currentEventLoop.Execute(_flushTask);
        }

        private void Flush()
        {
            if (inFlush) return;

            var scheduleAsync = false;
            for (var i = 0; i < 256; ++i)
            {
                if (!writeQueue.TryPeek(out Holder holder)) return;

                scheduleAsync = false;
                var done = false;
                inFlush = true;
                var p = holder.packet;

                for (var n = 0; n < 16; ++i)
                {
                    var writeBytes = DoWritePacket(p);
                    if (writeBytes < 0)
                    {
                        inFlush = false;
                        return;
                    }

                    if (writeBytes == 0)
                    {
                        scheduleAsync = true;
                        break;
                    }

                    if (!p.IsReadable())
                    {
                        done = true;
                        break;
                    }
                }

                if (!done)
                {
                    if (!ScheduleWrite(scheduleAsync, p))
                    {
                        scheduleAsync = false;
                        if (!p.IsReadable())
                        {
                            Dequeue();
                            inFlush = false;
                        }
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    Dequeue();
                    inFlush = false;
                }
            }

            if (!scheduleAsync && !inFlush)
            {
                _currentEventLoop.Execute(_flushTask);
            }
        }

        private bool ScheduleWrite(bool scheduleAsync, Packet p)
        {
            if (scheduleAsync)
            {
                return DoWriteAsync(p);
            }
            else
            {
                inFlush = false;
                _currentEventLoop.Execute(_flushTask);
                return true;
            }
        }

        private void Enqueue(Packet p)
        {
            writeQueue.Enqueue(new Holder(p));
            IncrementPendingOutboundBytes(p);
        }

        private void Dequeue()
        {
            if (writeQueue.TryDequeue(out Holder hodler))
            {
                var p = hodler.packet;
                hodler.packet = null;
                DecrementPendingOutboundBytes(p);
            }

        }

        private void WriteRestart()
        {
            _currentEventLoop.Execute(_flushTask);
        }

        protected void PostWrite(Packet packet)
        {
            if (!packet.IsReadable())
            {
                Dequeue();
                if (close == 1) return;
            }
            inFlush = false;
            _currentEventLoop.Execute(_flushTask);
        }

        private void DecrementPendingOutboundBytes(Packet p)
        {
            if (Interlocked.Add(ref waterMark, -p.writerIndex) <= LOW_WATER_MARK)
            {
                writtable = 1;
            }
        }

        private void IncrementPendingOutboundBytes(Packet p)
        {
            if (Interlocked.Add(ref waterMark, p.writerIndex) >= HIGH_WATER_MARK)
            {
                writtable = 0;
            }
        }

        public void Receive()
        {
            if (_currentEventLoop.InEventLoop)
            {
                _receiveTask();
            }
            else
            {
                _currentEventLoop.Execute(_receiveTask);
            }
        }

        protected void PostReceive(Packet packet)
        {
            void task()
            {
                if (packet.IsReadable())
                {
                    pipeline.Receive(packet);
                    if (close == 1) return;
                    DoReceive();
                }
                else
                {
                    Close();
                }
            }

            if (_currentEventLoop.InEventLoop)
            {
                task();
            }
            else
            {
                _currentEventLoop.Execute(task);
            }

        }

        private void DoReceive()
        {
            var packet = _pool.Rent();
            ReceiveInternal(packet);
        }

        public void Close()
        {
            if (_currentEventLoop.InEventLoop)
            {
                _closeTask();
            }
            else
            {
                _currentEventLoop.Execute(_closeTask);
            }
        }

        public void Release()
        {
            if (writeQueue == null) return;
            while (true)
            {
                var take = writeQueue.TryDequeue(out Holder output);
                if (!take) return;
                output.packet = null;
            }
        }

        public bool Writtable
        {
            get { return close == 0 && writtable == 1; }
        }

        public bool Closed
        {
            get { return close == 1; }
        }

        void Channel.UpdateSocket(Socket s)
        {
            socket = s;
            close = 0;
            reading = IDLE;
            inFlush = false;
            Receive();
            WriteRestart();
        }

        private void TriggerInActive()
        {
            pipeline.InActive();
        }

        ChannelPipeline Channel.Pipeline()
        {
            return pipeline;
        }

        private class Holder
        {
            public Packet packet;
            public Holder(Packet packet)
            {
                this.packet = packet;
            }
        }

        public EndPoint RemoteEndPoint()
        {
            return socket.RemoteEndPoint;
        }

        private void ReceiveInternal(Packet p)
        {
            try
            {
                receiveArgs.SetBuffer(p.buffer, p.writerIndex, p.Remain);
                receiveArgs.UserToken = p;
                if (!socket.ReceiveAsync(receiveArgs))
                {
                    ProcessReceiveCompleted();
                }
            }
            catch (Exception e)
            {
                logger.Warn(e, "read data error");
                Close();
            }
        }

        private void ProcessReceiveCompleted()
        {
            if (receiveArgs.SocketError != SocketError.Success)
            {
                Close();
                return;
            }

            var read = receiveArgs.BytesTransferred;
            var packet = (Packet)receiveArgs.UserToken;
            packet.writerIndex = packet.writerIndex + read;

            PostReceive(packet);
        }

        private bool ProcessWriteCompleted(bool pending)
        {
            if (writeArgs.SocketError != SocketError.Success)
            {
                Close();
                return false;
            }

            var written = writeArgs.BytesTransferred;
            var packet = (Packet)writeArgs.UserToken;
            packet.readerIndex = packet.readerIndex + written;

            if (pending)
            {
                PostWrite(packet);
            }
            return true;
        }

        private int DoWritePacket(Packet p)
        {
            try
            {
                var writeBytes = socket.Send(p.buffer, p.readerIndex, p.Len, SocketFlags.None, out SocketError error);
                if (error == SocketError.Success || error == SocketError.WouldBlock)
                {
                    if (writeBytes > 0)
                    {
                        p.readerIndex += writeBytes;
                    }
                    return writeBytes;
                }
                Close();
                return -1;
            }
            catch (Exception e)
            {
                logger.Warn(e, "send data error");
                Close();
                return -1;
            }
        }

        private bool DoWriteAsync(Packet p)
        {
            try
            {
                writeArgs.SetBuffer(p.buffer, p.readerIndex, p.Len);
                writeArgs.UserToken = p;
                if (!socket.SendAsync(writeArgs))
                {
                    return !ProcessWriteCompleted(false);
                }
            }
            catch (Exception e)
            {
                logger.Warn(e, "send data error");
                Close();
                return true;
            }
            return true;
        }
    }
}
