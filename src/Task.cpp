#include "../include/Task.h"
#include "../include/Fiber.h"

using namespace JLib;

void JLib::WaitGroup::WakeAll()
{
    
        std::vector<Task*> to_wake;
        {
            std::lock_guard<std::mutex> lock(mtx);
            to_wake.assign(waiters.begin(), waiters.end());
            waiters.clear();
        }
        for (auto* t : to_wake) {
            if (t->assignedFiber) t->assignedFiber->Resume();
        }
    
}
