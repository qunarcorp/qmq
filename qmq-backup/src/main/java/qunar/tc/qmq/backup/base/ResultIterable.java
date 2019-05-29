package qunar.tc.qmq.backup.base;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * User: zhaohuiyu Date: 3/22/14 Time: 10:29 PM
 */
public class ResultIterable<T extends Serializable> implements Iterable<T>, Serializable {
    private static final long serialVersionUID = -6106414829068194397L;

    private List<T> list = Lists.newArrayList();
    private Serializable next;

    public ResultIterable() {
        super();
    }

    public ResultIterable(List<T> list, Serializable next) {
        this.list = list;
        this.next = next;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

    public List<T> getList() {
        return list;
    }

    public void setNext(Serializable next) {
        this.next = next;
    }

    public Serializable getNext() {
        return next;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iter<T>(list);
    }

    private static class Iter<T extends Serializable> implements Iterator<T> {
        private final List<T> list;

        private int index;

        public Iter(List<T> list) {
            this.list = list;
        }

        @Override
        public boolean hasNext() {
            return index < list.size();
        }

        @Override
        public T next() {
            return list.get(index++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
