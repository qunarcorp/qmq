#ifndef PITOCIN_LINK_Node_H
#define PITOCIN_LINK_Node_H

#include <stack>

namespace Pitocin
{
template <typename T>
class StackPool
{
    std::stack<T *> s;

public:
    StackPool()
    {
    }
    ~StackPool()
    {
        while (!s.empty())
        {
            delete s.top();
            s.pop();
        }
    }

    inline bool empty()
    {
        return s.empty();
    }
    inline size_t size()
    {
        return s.size();
    }

    template <class... Args>
    void emplace(Args &&... args)
    {
        T *node = new T(std::forward<Args>(args)...);
        if (node)
        {
            s.push(node);
        }
    }

    inline void push(T *value)
    {
        s.push(value);
    }

    inline T *pop()
    {
        if (s.empty())
        {
            return nullptr;
        }
        T *top = s.top();
        s.pop();
        return top;
    }
};
} // namespace Pitocin

#endif