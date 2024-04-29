import os

def save_df_to_excel(df, code, custom_string, folder):
    if not os.path.exists(folder):
        os.makedirs(folder)
    filename = f"{code}{custom_string}.xlsx"
    path_file = folder + filename
    df.to_excel(path_file, index=True)