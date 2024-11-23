# Real Estate Transaction Analyzer 🏠

---

## Project Overview
The **Real Estate Transaction Analyzer** is a Streamlit-based tool designed to streamline the process of reviewing and labeling real estate transactions for newsworthiness. Developed as part of the **[JournalismAI Fellowship](https://www.journalismai.info/programmes/fellowship)** in collaboration with the Globe & Mail, Hearst, Gannett and E24, this tool empowers journalists and analysts to interact with machine learning output, refine predictions, and feed labeled data back into BigQuery.

---

## Features
1. **BigQuery Integration**:
   - Fetches data processed by a newsworthiness prediction model.
   - Allows users to label transactions directly in the app, saving feedback back to BigQuery.

2. **Interactive UI**:
   - Displays transaction details in an intuitive table. 
   - Provides filters for district, property type, price range, and more.

3. **Labeling and Feedback**:
   - Displays the details of an individual transaction.
   - Allows users to mark transactions as "Newsworthy" (👍) or "Not Newsworthy" (👎).
   - Allows users to add optional written feedback, all of which is updated in BigQuery.

---

## Sample CSV
Below is a sample CSV structure for upload:

| Transaction Details (JSON)                           | Property Description                                    | Newsworthy Alert                                      |
|------------------------------------------------------|-------------------------------------------------------|-----------------------------------------------------|
| {'number_of_buyers': 2, 'transaction_sum': 14100000} | This transaction involves a Duplex...                | The property price per square meter is 99% higher...|
| {'number_of_buyers': 1, 'transaction_sum': 42500}    | This transaction involves a unit in an apartment...  | The property price per square meter is 99% lower... |
| {'number_of_buyers': 1, 'transaction_sum': 20000000} | This transaction involves a Terraced house...        | The property price per square meter is 154% higher...|

---

## Getting Started
1. **Run the App**:
   - Install dependencies with `pip install -r requirements.txt`.
   - Start the app using `streamlit run app.py`.

2. **Set Up BigQuery**:
   - Ensure credentials for accessing BigQuery are correctly configured.
   - Update the project ID in the app configuration.

3. **Label Transactions**:
   - Interact with the app to view and label transactions.

---

## About JournalismAI Fellowship
The JournalismAI Fellowship explores the intersection of artificial intelligence and journalism, empowering reporters with AI tools. Learn more about the fellowship [here](https://www.journalismai.info/programmes/fellowship).
