use fields_macro::selected_fields;

#[selected_fields]
struct Foo {
    id: u32,
    car_name: String,
}

#[test]
fn it_works() {
    let foo = Foo {
        id: 1,
        car_name: "Hello".to_string(),
    };
    println!("{:?}", Foo::all_fields());
    let result = 2 + 2;
    assert_eq!(result, 4);
}
