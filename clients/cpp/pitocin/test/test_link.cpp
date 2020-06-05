#include <iostream>
#include <string>

#include "../src/LinkNode.hpp"

using namespace std;

class MyClass
{
public:
    string info;
    MyClass(string &&_info)
        : info(_info)
    {
        cout << "MyClass " << info << " construct" << endl;
    }
    ~MyClass()
    {
        cout << "MyClass " << info << " destruct" << endl;
    }

    void printInfo()
    {
        cout << info << endl;
    }
};

int main()
{
    Pitocin::LinkStack<MyClass> stack;
    stack.emplace("number1");
    stack.emplace("number2");
    MyClass *my = stack.pop();
    my->printInfo();
    MyClass *my1 = stack.pop();
    my1->printInfo();
    stack.push(my);
    stack.push(my1);
    return 0;
}