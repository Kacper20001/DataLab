import os
import cProfile
from memory_profiler import memory_usage

def profile_cpu(func, filename="cpu_profile.prof"):
    os.makedirs("data/profiling", exist_ok=True)
    output_path = os.path.join("data", "profiling", filename)

    profiler = cProfile.Profile()
    profiler.enable()
    func()  # wywołaj faktycznie funkcję (a nie przez exec)
    profiler.disable()
    profiler.dump_stats(output_path)

def profile_memory(func, filename="memory_profile.memlog"):
    os.makedirs("data/profiling", exist_ok=True)
    output_path = os.path.join("data", "profiling", filename)
    mem_usage = memory_usage(func, interval=0.1, timeout=None)
    with open(output_path, "w") as f:
        for usage in mem_usage:
            f.write(f"{usage}\n")
