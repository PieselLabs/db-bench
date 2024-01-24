fn main() {
    use std::fs::File;

    fn main() -> anyhow::Result<()> {
        let file = File::open("example.csv")?;
        println!("Hello, world!");
        Ok(())
    }

}
