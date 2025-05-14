# CDI Bonus Interest Calculation
This repository contains a data product designed to calculate and apply daily CDI bonus interest to users’ wallet balances. Built using Apache Spark (PySpark), it simulates a production-ready pipeline that reads change data capture (CDC) files, reconstructs daily wallet balances, applies business rules, and writes new interest transactions.

The solution emphasizes clean architecture, traceability, and fault tolerance, aligned with real-world data engineering standards — particularly important in the context of financial services.

Main features:

Generates a daily snapshot of wallet balances using CDC input data.

Calculates CDI interest on balances that meet defined criteria (amount > $100, idle for ≥24h).

Outputs interest transactions to be ingested into a transactional database.

Built with modular code, unit tests, and reproducible execution.

Designed for RecargaPay’s technical challenge, this project highlights how Spark can be used to build scalable, reliable, and auditable data pipelines.


Project Architecture Justification
This project has been structured to simulate a real-world data product that could be deployed into a production environment. The main design goals were modularity, traceability, testability, and clarity.

Modular Code (in src/): Business logic is separated into independent components (loading, snapshotting, interest calculation). This allows easier maintenance, unit testing, and scalability.

Raw and Output Data Separation (data/): Input and output are clearly separated to simulate a data lake or ingestion pipeline.

Reproducible Execution (run.py): The entire pipeline can be triggered from a single script to mimic a scheduled job or DAG task.

External Configuration (config.yaml): Parameterization is isolated from logic to facilitate testing and environment changes.

Automated Tests (tests/): Basic unit and end-to-end tests validate core components, demonstrating reliability — a key requirement for financial systems.

Documentation (README.md, docs/): Clear instructions and rationale are included to help future maintainers and reviewers understand the project and decisions made.

This structure ensures that even though the task is simplified, it models production-critical considerations like maintainability, visibility, and data traceability.