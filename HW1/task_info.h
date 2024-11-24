#pragma once

#include <cstddef>

constexpr double LB = 0.0;
constexpr double RB = 5.0;
constexpr size_t SEG_COUNT = 5;
constexpr size_t STEPS_PER_SEG = 1e6;
constexpr double SEG_LEN = (RB - LB) / SEG_COUNT;
constexpr double STEP_LEN = static_cast<double>(SEG_LEN) / STEPS_PER_SEG;

constexpr int BROADCAST_PORT = 8080;
constexpr int TCP_PORT = 9090;
constexpr char const* DISCOVER_MSG = "DISCOVER";
constexpr char const* ACK_MSG = "ACK";

struct SegResult {
    size_t seg_num;
    double result;
};
