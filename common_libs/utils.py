def read_secrets(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        line = file.readline()
        res = {}
        while line:
            _splited_line = line.strip().split('=')
            res[_splited_line[0]] = _splited_line[1]
            line = file.readline()
    return res
