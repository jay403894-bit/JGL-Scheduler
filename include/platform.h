
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

// Define these BEFORE including raylib or windows.h
#define NOGDI              // Suppress GDI macros like Rectangle and DrawText
#define NOUSER             // Suppress User macros like CloseWindow, ShowCursor
#include <windows.h>
#undef NOGDI
#undef NOUSER

// Now include Raylib
//#include "raylib.h"

// If you still have collisions, use the push/pop method
//#pragma push_macro("Rectangle")
//#undef Rectangle
//#pragma pop_macro("Rectangle")

using affinity_mask_t = DWORD_PTR;
using native_handle_t = HANDLE;