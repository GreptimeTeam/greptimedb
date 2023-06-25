## Generate orc data

```bash
python3 -m venv venv
venv/bin/pip install -U pip
venv/bin/pip install -U pyorc

./venv/bin/python write.py

cargo test
```
