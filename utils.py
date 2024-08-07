import os

def save_df_to_excel(df, code, custom_string, folder):
    if not os.path.exists(folder):
        os.makedirs(folder)
    filename = f"{code}{custom_string}.xlsx"
    path_file = folder + filename
    #df.to_excel(path_file, index=True)
    df.to_excel(path_file, index=False) # 2024.7.4 변경. save_dfs_to_excel와의 통일성 맞추기 위해

def save_dfs_to_excel(dfs_dict, custom_string, folder):
    if not os.path.exists(folder):
        os.makedirs(folder)
    for code, df in dfs_dict.items():
        filename = f"{code}{custom_string}.xlsx"
        path_file = folder + filename
        df.to_excel(path_file, index=False)

# txt 파일에 데이터를 쓰는 함수
def save_list_to_file(data_list, filename):
    with open(filename, 'w') as file:  # 'w' 모드: 쓰기 전용으로 파일을 엽니다. 파일이 이미 존재하는 경우, 파일을 새로 만들어 기존의 내용을 삭제
        for item in data_list:
            file.write(f"{item}\n")

# txt 파일에 데이터를 추가하는 함수
def save_list_to_file_append(data_list, filename):
    with open(filename, 'a') as file:  # 'a' 모드는 파일에 내용을 추가합니다
        for item in data_list:
            file.write(f"{item}\n")

# folder_path에서 keyword를 갖는 파일이름의 목록을 리턴
def find_files_with_keyword(folder_path, keyword):
    files = os.listdir(folder_path)
    files_with_keyword = [file for file in files if keyword in file]
    return files_with_keyword