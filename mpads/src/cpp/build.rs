fn main() {
    cc::Build::new()
        .cpp(true)
        .flag("-O3")
        .file("src/cpp/b48multimap.cpp")
        .compile("b48multimap.a");
}
