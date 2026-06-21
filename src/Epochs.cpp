#include "../include/Epochs.h"
#include <atomic>
#include <vector>

namespace T_Threads {
	thread_local size_t thread_id = 0;
	thread_local std::vector<RetiredAlloc> retired;
}