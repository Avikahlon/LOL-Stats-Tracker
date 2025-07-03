# 🧠 League of Legends Stats Tracker

A full-stack web application that tracks and visualizes League of Legends player statistics by scraping data from a third-party website, storing it in a structured database, and displaying it on a modern frontend interface.

## 🔍 Project Overview

Due to limitations in publicly available APIs for League of Legends data, this project bypasses those restrictions by scraping a trusted external stats site. The extracted data is cleaned and stored in a database, then served through a custom frontend to allow users to search, filter, and compare players across games, roles, and tournaments.

## ⚙️ Features

- 🕷️ Web scraper to collect player stats (KDA, CS, win rate, etc.)
- 💾 Backend with database storage (PostgreSQL or MySQL)
- 🌐 REST API or GraphQL layer to serve the data
- 🎨 Frontend built with React + Vite for an interactive UI
- 📊 Interactive charts and tables for data visualization
- 🔍 Player search and comparison functionality
- 🧰 Admin panel for updating/syncing scraped data

## 🧱 Tech Stack

### Backend
- Python (BeautifulSoup, Requests, FastAPI or Flask)
- PostgreSQL or MySQL
- SQLAlchemy or psycopg2
- AWS (optional – S3, RDS)

### Frontend
- React + Vite
- Tailwind CSS or Styled Components
- Chart.js or Recharts

### Dev Tools
- Git + GitHub
- Docker (optional)
- VS Code

## 📁 Project Structure

