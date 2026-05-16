#include <cstdint>
#include <iostream>

int main() {
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(nullptr);
    int64_t a = 0, b = 0;
    std::cin >> a >> b;
    std::cout << (a + b) << '\n';
    return 0;
}
