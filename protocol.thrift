service Watcher {
    void buy_signal(1: string symbol, 2: double price, 3: double init_price),
    void sell_signal(1: string symbol, 2: double price, 3: double init_price),
    list<string> get_task(1: i32 num),
    string alive()
}

service Dealer {
    void buy_signal(1: string symbol, 2: double price, 3: double init_price),
    void sell_signal(1: string symbol, 2: double price, 3: double init_price),
    string alive()
}