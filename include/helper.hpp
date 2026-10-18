#pragma once

#include <exception>
#include <iostream>

namespace helper
{
// Completion token callback to rethrow exceptions from coroutines
struct RethrowFirstArg
{
    template <class... T>
    void operator()(std::exception_ptr ep, T&&...) const
    {
        if (ep)
        {
            std::rethrow_exception(ep);
        }
    }

    template <class... T>
    void operator()(T&&...) const
    {
    }
};
}  // namespace helper
