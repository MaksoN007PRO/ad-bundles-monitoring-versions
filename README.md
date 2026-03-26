# Versions of Arenadata Bundles Components

## 📌 Описание

Веб-приложение на базе **Streamlit**, представляющее собой инструмент для отображения версий компонентов бандлов Arenadata.

Приложение:

* скачивает `.tgz` бандлы с официальных репозиториев Arenadata;
* извлекает версии компонентов из YAML-файлов внутри архивов;
* кеширует результаты в `cache.json`;
* отображает данные в удобном UI.

## ⚙️ Запуск без Docker (локально)

### 1. Создать виртуальное окружение:

```bash
python3 -m venv venv
source venv/bin/activate
```

---

### 2. Установить зависимости:

```bash
pip install -r requirements.txt
```

---

### 3. Запустить:

```bash
streamlit run ad-bundles-parser.py
```

---

### 4. Открыть в браузере:

```
http://localhost:8501
```

---

## 🐳 Запуск через Docker

### 1. Сборка и запуск:

```bash
docker compose up -d --build
```

---

### 2. Проверка:

```
http://localhost:8501
```

---
