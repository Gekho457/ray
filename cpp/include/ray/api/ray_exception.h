#pragma once

#include <exception>
#include <string>

namespace ray {
namespace api {

class RayException : public std::exception {
 public:
  RayException(const std::string &msg) : msg_(msg){};

  const char *what() const noexcept override { return msg_.c_str(); };

  std::string msg_;
};

class RayFunctionNotFound : public RayException {
 public:
  RayFunctionNotFound(const std::string &msg) : RayException(msg){};
};
}  // namespace api
}  // namespace ray