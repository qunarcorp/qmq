using System;
using System.Collections.Generic;

namespace Qunar.TC.Qmq.Client.Transport
{
    interface Pool
    {
        Packet Rent();

        void Return(Packet item);
    }

    class ByteBufferPool : Pool
    {
        private readonly int _size;

        private readonly Queue<Packet> _buffer;

        private readonly object _lock;

        private readonly int _bufferSize;

        public ByteBufferPool(int size, int bufferSize)
        {
            _size = size;
            _lock = new object();
            _bufferSize = bufferSize;
            _buffer = new Queue<Packet>(bufferSize);
        }

        public Packet Rent()
        {
            lock (_lock)
            {
                if (_buffer.Count == 0)
                {
                    var result = new Packet(new byte[_size], 0, 0)
                    {
                        Pool = this
                    };
                    return result;
                }
                return _buffer.Dequeue();
            }
        }

        public void Return(Packet item)
        {
            lock (_lock)
            {
                if (_buffer.Count > _bufferSize)
                {
                    return;
                }
                item.readerIndex = 0;
                item.writerIndex = 0;
                _buffer.Enqueue(item);
            }
        }
    }

    class MockPool : Pool
    {
        private readonly int _size;

        public MockPool(int size)
        {
            _size = size;
        }

        public Packet Rent()
        {
            return new Packet(new byte[_size], 0, 0);
        }

        public void Return(Packet item)
        {

        }
    }
}
