//timer.h
#ifndef W_TIMER_H
#define W_TIMER_H
 
#include <iostream>
#include <string>
#include <chrono>
#include <fstream>
 
class Timer {
public:
 
    Timer():_name("Time elapsed:") {
        // restart();
    }
 
    explicit Timer(const std::string &name):_name(name + ":") {
        // restart();
    }

    Timer(const std::string &name, const std::string file_name):_name(name + ":") {
        outfile.open(file_name, std::ios::out | std::ios::app);
    }
 
    /**
    * 启动计时
    */
    inline void start() {
        _start_time = std::chrono::steady_clock::now();
    }

    inline void restart() {
        _start_time = std::chrono::steady_clock::now();
    }
 
    /**
    * 结束计时
    * @return 返回ms数
    */
    inline double elapsed(bool restart = false) {
        _end_time = std::chrono::steady_clock::now();
        std::chrono::duration<double> diff = _end_time-_start_time;
        if(restart)
            this->restart();
        return diff.count()*1000;
    }
 
    /**
     * 打印时间并重启计时器
     * @param tip 提示
     */
    void rprint(const std::string &tip){
        print(true,tip,true,false);
    }
 
    /**
    * 打印时间
    * @param reset 输出之后是否重启计时器，true重启，false不重启
    * @param unit_ms true是ms，false是s
    * @param tip 输出提示
    * @param kill 输出之后是否退出程序，true退出，false不退出
    */
    void print(bool reset = false, const std::string &tip = "",
             bool unit_ms = true, bool kill = false
    ) {
        if (unit_ms) {
            if (tip.length() > 0)
                std::cout << tip+":" << elapsed() << "ms" << std::endl;
            else
                std::cout << _name << elapsed() << "ms" << std::endl;
        } else {
            if (tip.length() > 0)
                std::cout << tip+":" << elapsed() / 1000.0 << "s" << std::endl;
            else
                std::cout << _name << elapsed() / 1000.0 << "s" << std::endl;
        }
 
        if (reset)
            this->restart();
 
        if (kill)
            exit(5);
    }

    void log(bool reset = false, const std::string &tip = "",
             bool unit_ms = true, bool kill = false
    ) {
        if (unit_ms) {
            if (tip.length() > 0)
                outfile << tip+":" << elapsed() << "ms" << std::endl;
            else
                outfile << _name << elapsed() << "ms" << std::endl;
        } else {
            if (tip.length() > 0)
                outfile << tip+":" << elapsed() / 1000.0 << "s" << std::endl;
            else
                outfile << _name << elapsed() / 1000.0 << "s" << std::endl;
        }
 
        if (reset)
            this->restart();
 
        if (kill)
            exit(5);
    }
    
 
 
private:
    std::chrono::steady_clock::time_point _start_time;
    std::chrono::steady_clock::time_point _end_time;
    std::string _name;
    std::fstream outfile;
}; // timer
 
#endif //W_TIMER_H