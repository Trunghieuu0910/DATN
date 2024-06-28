import json

import pandas as pd
import requests
import matplotlib.pyplot as plt
import seaborn as sns

import streamlit as st

st.title("Classify wallet addresses by geographical area")
st.set_option('deprecation.showPyplotGlobalUse', False)


text_input = st.text_input("Wallet Adresses")

def format_label(label):
    # Chuyển dấu _ thành dấu space, viết hoa chữ cái đầu
    formatted_label = label.replace('_', ' ').title()
    # Nếu label là jp_kr_cn thì đổi thành East Asia
    if formatted_label == 'Jp Kr Cn':
        return 'East Asia'
    else:
        return formatted_label

def remove_outliers(df):
    for column in df.columns:
        if df[column].dtype.kind in 'bifc':  # Kiểm tra nếu cột là kiểu số
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    return df

def get_addresses_regional(addresses):
    addresses = str(addresses)
    addresses = addresses.split(",")
    addresses = [address.strip() for address in addresses]
    headers = {
        'Content-Type': 'application/json'
    }
    data = {'addresses': addresses}
    json_data = json.dumps(data)

    response = requests.post(f'http://0.0.0.0:8096/addresses/', data=json_data, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Có lỗi xảy ra khi gọi API")

def get_transactions_address(addresses):
    addresses = str(addresses)
    addresses = addresses.split(",")
    addresses = [address.strip() for address in addresses]
    headers = {
        'Content-Type': 'application/json'
    }
    data = {'addresses': addresses}
    json_data = json.dumps(data)

    response = requests.post(f'http://0.0.0.0:8096/transactions/', data=json_data, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Có lỗi xảy ra khi gọi API")

# Tạo một nút bấm
if st.button("Find"):
    if text_input:
        regional = get_addresses_regional(text_input)
        # Tạo DataFrame từ dữ liệu
        df = pd.DataFrame(regional)
        df['regional'] = df['regional'].apply(format_label)

        st.subheader("Regional:")
        st.dataframe(df, width=1100)
        fig, axes = plt.subplots(1, 2, figsize=(10, 5))

        st.subheader("Distributed in each region: ")
        df['regional'].value_counts().plot(kind='bar', ax=axes[0])
        axes[0].set_title('')

        df['regional'].value_counts().plot(kind='pie', ax=axes[1], autopct='%1.1f%%')
        axes[1].set_title('')
        plt.tight_layout()
        st.pyplot(fig)
        plt.clf()
        transactions = get_transactions_address(addresses=text_input)
        tx_data = {}
        mean_data = {}
        for region, v in transactions.items():
            region = format_label(region)
            tx_data[region] = v.get('transactions')
            mean_data[region] = v.get('means')

        st.subheader("Distributed of transactions time: ")
        for region, values in tx_data.items():
            st.write(f'{region}')
            if values:  # Kiểm tra xem dữ liệu tx_data có tồn tại hay không
                plt.bar(values.keys(), values.values())
                plt.xlabel('')
                plt.ylabel('')
                plt.xticks(rotation=45)
                st.pyplot(plt)
                plt.clf()
            else:
                st.write(f"No data for {region}")

        st.subheader("Distributed of mean transactions: ")
        df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in mean_data.items()]))
        df = remove_outliers(df)
        # Vẽ boxplot
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.boxplot(data=df, ax=ax)
        ax.set_title("")

        # Hiển thị biểu đồ trên giao diện Streamlit
        st.pyplot(fig)
    else:
        st.write("Please enter valid blockchain address")
