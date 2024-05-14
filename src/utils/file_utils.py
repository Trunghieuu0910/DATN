import json
import csv


def open_json_file_to_dict(file_path):
    with open(file_path, 'r') as file:
        json_data = file.read()
    list_of_dicts = json.loads(json_data)

    return list_of_dicts


def write_json_file(file_path, list_dict):
    with open(file_path, 'w') as file:
        json.dump(list_dict, file, indent=4)


def write_error_file(file_path, word):
    with open(file_path, 'a') as file:
        file.write(word)
        file.write('\n')


def write_flat_dict_to_csv(file_path, flattened_dict):
    with open(file_path, 'a', newline='') as csvfile:
        # Khởi tạo đối tượng DictWriter
        writer = csv.DictWriter(csvfile, fieldnames=flattened_dict.keys())

        # Kiểm tra nếu file CSV trống, ghi header
        if csvfile.tell() == 0:
            writer.writeheader()

        # Ghi dữ liệu từ flattened_dict vào file CSV
        writer.writerow(flattened_dict)


def read_csv_to_list_of_dicts(file_path):
    data_list = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_list.append(row)
    return data_list


def read_remove_file(file_path):
    res = []
    with open(file_path, 'r') as remove_file:
        for line in remove_file:
            line = line.strip()
            line = line.split(":")[0]
            line = line.replace('"', '')
            line = line.strip()
            res.append(str(line))

    return res