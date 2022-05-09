use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::time::Instant;

fn read(filename: &str) -> Result<Vec<HashMap<String, String>>, csv::Error> {
    let file = File::open(filename)?;
    let mut reader = csv::Reader::from_reader(file);
    reader.deserialize().collect()
}

fn aggregate_1<'a>(records: &'a [HashMap<String, String>], keys: &[&str; 5], value: &str) -> Result<HashMap<Vec<&'a String>, u32>, Box<dyn Error>> {
    let mut aggregated = HashMap::new();
    for i in 0..records.len() {
        let k = keys.iter().map(|&k| &records[i][k]).collect::<Vec<_>>();
        let v = records[i][value].parse::<u32>()?;

        *aggregated.entry(k).or_default() += v;
    }

    Ok(aggregated)
}

fn aggregate_2<'a>(records: &'a [HashMap<String, String>], keys: &[&str; 5], value: &str) -> Result<HashMap<&'a String, HashMap<&'a String, HashMap<&'a String, HashMap<&'a String, HashMap<&'a String, u32>>>>>, Box<dyn Error>> {
    let mut aggregated: HashMap<&'a String, HashMap<&'a String, HashMap<&'a String, HashMap<&'a String, HashMap<&'a String, u32>>>>> = HashMap::new();

    for i in 0..records.len() {
        let v = records[i][value].parse::<u32>()?;

        *aggregated.entry(&records[i][keys[0]]).or_default()
            .entry(&records[i][keys[1]]).or_default()
            .entry(&records[i][keys[2]]).or_default()
            .entry(&records[i][keys[3]]).or_default()
            .entry(&records[i][keys[4]]).or_default() += v;
    }

    Ok(aggregated)
}

fn main() -> Result<(), Box<dyn Error>> {
    let records = read("sgl-arbres-urbains-wgs84.csv")?;
    let keys = ["quartier", "site", "genre_arbre", "classe_age", "delai_annee_programmation_2"];
    let value = "hauteur";

    let now = Instant::now();
    for _ in 0..10000 {
        let _ = aggregate_1(&records, &keys, &value)?;
    }
    println!("aggregate_1 took {}ms", now.elapsed().as_millis());

    let now = Instant::now();
    for _ in 0..10000 {
        let _ = aggregate_2(&records, &keys, &value)?;
    }
    println!("aggregate_2 took {}ms", now.elapsed().as_millis());
    Ok(())
}
