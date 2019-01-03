// yuzhaohui
// 8/1/2017
using System;

namespace Qunar.TC.Qmq.Client.Util
{
    class StateChangedArgs : EventArgs
    {
        public static readonly StateChangedArgs Connected = new StateChangedArgs(State.Connected);

        public static readonly StateChangedArgs DisConnect = new StateChangedArgs(State.DisConnect);

        public static readonly StateChangedArgs Close = new StateChangedArgs(State.Close);

        public static readonly StateChangedArgs ConnectFailed = new StateChangedArgs(State.ConnectFailed);

        private readonly State state;

        public StateChangedArgs(State state)
        {
            this.state = state;
        }

        public State State
        {
            get { return this.state; }
        }
    }

    enum State
    {
        Connected,
        DisConnect,
        ConnectFailed,
        Close
    }
}
