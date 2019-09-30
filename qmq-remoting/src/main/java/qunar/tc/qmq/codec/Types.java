package qunar.tc.qmq.codec;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class Types {

    public static ParameterizedType newParameterizedType(Type ownerType, Type rawType, Type[] argumentTypes) {
        return new ParameterizedTypeImpl(ownerType, rawType, argumentTypes);
    }

    private static class ParameterizedTypeImpl implements ParameterizedType {

        private Type ownerType;
        private Type rawType;
        private Type[] argumentTypes;

        public ParameterizedTypeImpl(Type ownerType, Type rawType, Type[] argumentTypes) {
            this.argumentTypes = argumentTypes;
            this.rawType = rawType;
            this.ownerType = ownerType;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return argumentTypes;
        }

        @Override
        public Type getRawType() {
            return rawType;
        }

        @Override
        public Type getOwnerType() {
            return ownerType;
        }

        @Override
        public String getTypeName() {
            return rawType.getTypeName();
        }
    }
}
