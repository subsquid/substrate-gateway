file = open('Cargo.toml')
for line in file:
    if 'version' in line:
        _, version, _ = line.split('"')
        print(version)
        break
file.close()
