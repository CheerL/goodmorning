service Watcher {
    void buy_signal(1: string symbol, 2: double price),
    list<string> get_task(1: i32 num),
    string alive()
}