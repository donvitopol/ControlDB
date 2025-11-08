# 🧩 ControlDB

**ControlDB** is a lightweight, modular Python framework for managing local or multi-layered databases and Excel-based data workflows.  
It is designed for researchers, engineers, and developers who need structured control over experimental or analytical data — without the overhead of large database systems.

---

## 🚀 Features

- **🔐 User & Access Management**  
  Built-in system for creating root and user databases with authentication (username/password).  

- **🗂️ Dynamic Database Creation**  
  Easily create and link multiple SQLite or MS Access (`.mdb`) databases dynamically.  

- **📊 Excel File Management**  
  Includes a high-level `ExcelManager` for creating, reading, updating, and merging Excel files with clean logging.  

- **⚙️ Utility Layer (`UtilManager`)**  
  Common utility functions for database operations and logging integration.  

- **🧪 Full Unit Testing Support**  
  Cleanly separated test suite (using `unittest`) for reliable and maintainable development.

---

## 📁 Project Structure


ControlDB/
│
├── controldb/
│ ├── init.py
│ ├── controldb_manager.py
│ ├── excel_manager.py
│ ├── util_manager.py
│ ├── pretty_logger.py
│ └── ...
│
├── tests/
│ ├── init.py
│ ├── test_excel_manager.py
│ ├── test_controldb_manager.py
│ └── helpers.py
│
└── README.md


---

## 🧱 Example Usage

```python
from ControlDB.excel_manager import ExcelManager

excel = ExcelManager()
excel.create_file("data/test.xlsx")
excel.upload_dataframe("data/test.xlsx", df, sheet_name="Results")

print(excel.get_columns("data/test.xlsx"))

🧰 Requirements

Python 3.10+

pandas

openpyxl

SQLAlchemy

Install dependencies:

pip install pandas openpyxl sqlalchemy

🧪 Running Tests

All test files are included under tests/.
Run all unit tests with:

pytest


or:

python -m unittest discover

🧑‍💻 Development

To clone and start developing:

git clone https://github.com/donvitopol/ControlDB.git
cd ControlDB
pip install -r requirements.txt

🧾 Version Control Workflow

Create a new branch for each feature or fix:

git checkout -b feature/my-feature


Commit your changes:

git commit -m "Add feature X"


Push and open a pull request:

git push origin feature/my-feature

📜 License

MIT License © 2025 — Don Vito Pol

⭐ If you find this project useful, give it a star on GitHub!

---

Wil je dat ik er ook **badges** aan toevoeg (zoals Python version, build status, license, en coverage)?  
Dat maakt de README visueel aantrekkelijker en professioneler voor GitHub.