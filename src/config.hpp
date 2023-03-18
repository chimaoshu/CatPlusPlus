#ifndef __CONFIG_HPP__
#define __CONFIG_HPP__

#include <fstream>
#include <map>
#include <sstream>
#include <string>

#include "src/util.hpp"

class ConfigReader {
private:
  std::map<std::string, std::string> config_;

  // 读取键值config，数据以空格隔开
  void read_kv_config(const std::string &path) {
    std::ifstream file(path);
    if (!file.is_open())
      UtilError::error_exit("config file not exits", false);

    // 读取行
    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
      line_num++;

      // 找'='
      int spliter = line.find_first_of('=');
      if (spliter == std::string::npos)
        UtilError::error_exit("invalid format: '=' missing", false);
      else if (line.find_last_of('=') != spliter)
        UtilError::error_exit("invalid format: multiple '=' in line " +
                                  std::to_string(line_num),
                              false);

      // KV
      std::string key = UtilString::strip(line.substr(0, spliter));
      std::string value = UtilString::strip(
          line.substr(spliter + 1, line.size() - spliter - 1));

      // 忽略注释和空行
      if (key.front() == '#' || key.empty())
        continue;

      // 有key没有value
      if (!key.empty() && value.empty())
        UtilError::error_exit("line " + std::to_string(line_num) +
                                  " has key but no value",
                              false);

      // 重复键
      if (config_.find(key) != config_.end())
        UtilError::error_exit("duplicate key \"" + key + "\" in line " +
                                  std::to_string(line_num),
                              false);

      config_[key] = value;
    }

    file.close();
  }

public:
  ConfigReader(std::string path) { read_kv_config(path); }
  std::string get(std::string key, const std::string &default_value = "") {
    auto it = config_.find(key);
    if (it != config_.end())
      return it->second;
    else
      return default_value;
  }
  std::string force_get(std::string key) {
    auto it = config_.find(key);
    if (it == config_.end())
      UtilError::error_exit("key not exist: " + key, false);
    return it->second;
  }
};

// 全局且单例的Config类
class config {
private:
  static ConfigReader &get_config() {
    static ConfigReader reader("./app.conf");
    return reader;
  }

public:
  static std::string get(std::string key,
                         const std::string &default_value = "") {
    return get_config().get(key, default_value);
  }
  static std::string force_get(std::string key) {
    return get_config().force_get(key);
  }
};

#endif // __CONFIG_HPP__