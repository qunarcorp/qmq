using NLog;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;

namespace Qunar.TC.Qmq.Client.Transport
{
    /// <summary>
    /// 长连接的client
    ///
    /// </summary>
    internal sealed class TransportClient : AbstractHandler, Client
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// 最大递增重连次数，小于这个次数每次重连间隔递增，大于这个次数每次重连间隔稳定为5分钟
        /// </summary>
        private const int MaxReconnectBackoff = 100;
        private const int ReconnectInterval = 5 * 60 * 1000;

        private static readonly IList<TransportClient> Clients = new List<TransportClient>();

        private readonly Encoder _encoder;
        private readonly Decoder _decoder;

        private readonly IKeepliveStrategy _keepliveStrategy;
        private readonly bool _autoReConnect;
        private int _readBufferSize;
        private Processor _processor;

        private readonly Random _rnd = new Random();

        private readonly object _connectedGuard = new object();
        private byte _writtable = 0;
        private volatile Channel _channel;
        private volatile bool _close = false;
        private int _init = 0;

        /// <summary>
        /// 当前重连次数
        /// </summary>
        private volatile int _reconnects = 0;

        private long _lastWrite = DateTime.Now.ToTime();
        private long _lastRead = DateTime.Now.ToTime();
        private long _lastConnect = DateTime.Now.ToTime();

        private EventHandler<StateChangedArgs> _stateChanged;
        public event EventHandler<StateChangedArgs> StateChanged
        {
            add
            {
                lock (_connectedGuard)
                {
                    if (_writtable == 1)
                    {
                        value(this, StateChangedArgs.Connected);
                    }
                    _stateChanged += value;
                }
            }

            remove => _stateChanged -= value;
        }

        public TransportClient(string host, int port, Encoder encoder, Decoder decoder, IKeepliveStrategy keepliveStrategy, bool autoReConnect = true, int readBufferSize = 256)
        {
            Host = host;
            Port = port;
            _encoder = encoder;
            _decoder = decoder;
            _keepliveStrategy = keepliveStrategy;
            _autoReConnect = autoReConnect;
            _readBufferSize = readBufferSize;
        }

        public void Connect()
        {
            if (Interlocked.CompareExchange(ref _init, 1, 0) != 0)
            {
                return;
            }
            DoConnect();
            Add(this);
        }

        private void DoConnect()
        {
            if (_close) return;
            try
            {
                var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                {
                    Blocking = false,
                    NoDelay = true,
                    SendBufferSize = 65535,
                    ReceiveBufferSize = 65535
                };
                s.BeginConnect(Host, Port, result =>
                {
                    try
                    {
                        s.EndConnect(result);
                        Thread.VolatileWrite(ref _lastConnect, DateTime.Now.ToTime());
                        _reconnects = 0;
                        if (_channel == null)
                        {
                            _channel = new SocketChannel(s, _readBufferSize);
                            InitContext();
                            _channel.Receive();
                        }
                        else
                        {
                            _channel.UpdateSocket(s);
                        }

                        EventHandler<StateChangedArgs> receivers;
                        lock (_connectedGuard)
                        {
                            Thread.VolatileWrite(ref _writtable, 1);
                            receivers = _stateChanged;
                        }
                        TriggerStateChangedOn(receivers, StateChangedArgs.Connected);
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex);
                        TriggerStateChanged(StateChangedArgs.ConnectFailed);
                        ReConnect();
                    }
                    finally
                    {
                    }
                }, null);
            }
            catch (Exception)
            {
                TriggerStateChanged(StateChangedArgs.ConnectFailed);
                ReConnect();
            }
        }

        private void ReConnect()
        {
            if (!_autoReConnect)
            {
                _close = true;
                TriggerStateChanged(StateChangedArgs.Close);
                return;
            }
            if (_close) return;

            Scheduler.INSTANCE.Schedule(DoConnect, ComputeNextConnectTime());
        }

        private DateTime ComputeNextConnectTime()
        {
            if (_reconnects++ <= MaxReconnectBackoff)
            {
                return DateTime.Now.AddMilliseconds(1000 + _reconnects * 1000);
            }
            return DateTime.Now.AddMilliseconds(ReconnectInterval + _rnd.Next(0, 2000));
        }

        public void Send(object req)
        {
            try
            {
                Thread.VolatileWrite(ref _lastWrite, DateTime.Now.ToTime());
                _channel.Write(req);
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
                throw;
            }
        }

        public override void Receive(HandlerContext context, object msg)
        {
            Thread.VolatileWrite(ref _lastRead, DateTime.Now.ToTime());
            _processor?.Process(context, msg);
        }

        private void InitContext()
        {
            var pipeline = _channel.Pipeline();
            pipeline.AddLast(new DecodeHandler(_decoder));
            pipeline.AddLast(new EncodeHandler(_encoder));
            pipeline.AddLast(this);
        }

        private static void Add(TransportClient client)
        {
            lock (Clients)
            {
                Clients.Add(client);
            }
        }

        public void Close()
        {
            Thread.VolatileWrite(ref _writtable, 0);
            if (_close) return;

            _close = true;
            TriggerStateChanged(StateChangedArgs.Close);
        }

        public void CloseChannel()
        {
            _channel?.Close();
        }

        public bool IsClose => _close;

        public Processor Processor
        {
            set => _processor = value;
        }

        public override void InActive(HandlerContext context)
        {
            Thread.VolatileWrite(ref _writtable, 0);
            TriggerStateChanged(StateChangedArgs.DisConnect);

            if (!_autoReConnect)
            {
                _close = true;
                TriggerStateChanged(StateChangedArgs.Close);
            }

            if (_close)
            {
                lock (Clients)
                {
                    Clients.Remove(this);
                }

                _channel.Release();
                return;
            }

            Scheduler.INSTANCE.Schedule(DoConnect, DateTime.Now.AddMilliseconds(_rnd.Next(0, 500)));
        }

        private void TriggerStateChanged(StateChangedArgs args)
        {
            _stateChanged?.Invoke(this, args);
        }

        private void TriggerStateChangedOn(EventHandler<StateChangedArgs> receivers, StateChangedArgs args)
        {
            receivers?.Invoke(this, args);
        }

        public bool Writtable => !_close && _writtable == 1 && _channel != null && _channel.Writtable;

        static TransportClient()
        {
            var thread = new Thread(obj =>
            {
                while (true)
                {
                    try
                    {
                        Keeplive();
                    }
                    catch
                    {
                        // ignored
                    }

                    Thread.Sleep(300);
                }
            })
            {
                Name = "qmq-client-keeplive-thread",
                IsBackground = true
            };
            thread.Start();
        }

        private static void Keeplive()
        {
            lock (Clients)
            {
                foreach (var client in Clients)
                {
                    client.CheckStatus();
                }
            }
        }

        private void CheckStatus()
        {
            _keepliveStrategy.CheckStatus(this, _lastWrite, _lastRead, _lastConnect);
        }

        public bool IsConnected()
        {
            return _channel != null && !_channel.Closed;
        }

        public int ReadBufferSize
        {
            set => _readBufferSize = value;
        }

        public string Host { get; }

        public int Port { get; }
    }
}