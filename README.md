# E-Commerce Data Analytics Pipeline - groupe Studio De Felice 💃

This BIG-DATA project implements a data pipeline for e-commerce trend analysis, from ingestion to machine learning predictions.

## 🌟 Features

- **Data Ingestion Service**: Automated data collection and storage system
- **Data Preprocessing**: Robust data cleaning and transformation pipeline
- **Machine Learning Module**: Predictive analytics and pattern recognition
- **SQL Analytics**: Advanced querying and business intelligence capabilities
- **Distributed Computing**: Built on Apache Spark for scalable processing

## 🏗️ Architecture

The project is built using a microservices architecture, with each component running in its own container:

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────┐
│  Data Ingestion │ ──► │  Preprocessing   │ ──► │  ML Module     │
└─────────────────┘     └──────────────────┘     └────────────────┘
                                                         │
                                                         ▼
                                               ┌────────────────┐
                                               │  SQL Analytics │
                                               └────────────────┘
```

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose
- Git
- 8GB+ RAM recommended

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/big-data-pipeline-group-Studio-de-Felice.git
cd big-data-pipeline-group-Studio-de-Felice
```

2. Start the services:
```bash
docker-compose up --build
```

## 🛠️ Technical Stack

- **Apache Spark**: Distributed computing engine
- **Python**: Primary programming language
- **Docker**: Containerization
- **PySpark**: Python API for Spark
- **Pandas**: Data manipulation and analysis
- **Scikit-learn**: Machine learning algorithms
- **Matplotlib/Seaborn**: Data visualization

## 📊 Data Pipeline

1. **Data Ingestion**
   - Automated data collection
   - Data validation and initial quality checks
   - Raw data storage

2. **Data Preprocessing**
   - Data cleaning and normalization
   - Feature engineering
   - Missing value handling

3. **Machine Learning**
   - Predictive modeling
   - Pattern recognition
   - Model evaluation and validation

4. **Analytics**
   - Business intelligence queries
   - Performance metrics
   - Trend analysis

## 📈 Performance

- Capable of processing millions of records
- Distributed computing for scalable performance
- Real-time analytics capabilities

## 👥 Team

- Studio de Felice 💃

For any questions or feedback, please contact the team at 

- [david.chane-yock-nam@epita.fr](mailto:david.chane-yock-nam@epita.fr)
- [pierre-louis.favreau@epita.fr](mailto:pierre-louis.favreau@epita.fr)
- [martin.natale@epita.fr](mailto:martin2.natale@epita.fr)
- [samy.amine@epita.fr](mailto:samy.amine@epita.fr)
- [vincent.saunier@epita.fr](mailto:vincent.saunier@epita.fr)

