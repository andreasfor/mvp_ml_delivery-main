# mvp_of_a_ml_deliver
The README file is written more like a diary with a touch of documentation at the moment. Where I can sum up my thoughts and what I have learned.

## Aim of the project
The aim of the project was twofold. First, to try a bunch of methods, tools, approaches, techniques, you name it, that I have come across during my first year as a consultant but perhaps have not had the chance to try until now.
The time cap was roughly three weeks, so I want to emphasize that nothing in this project is exhaustive; it is more of a quick sneak peak.

The second aim of the project was to produce a project that a junior colleague could easily follow so they (count me in) could see what an end-to-end minimal viable product of a machine learning delivery may look like. At the same time, be of sufficient quality to a senior Data Scientist/AI Architect (Jonas Mellin), a senior Data Engineer (Johan Öhman) and a Databricks Champion (Alexander Mafi). The term sufficient here means, if they were to open up this project at a client for the first time, they would say, "OK, maybe not how I would have done it. But, OK" and accept it, rather than chase perfection. In short, for this project, simplicity is rated higher than perfection.   

## What I was trying to build
An ETL flow that reads from an external Azure Data Lake Storage Gen2 using a Databricks function called Autoloader while storing secrets in Azure Key Vault. Once the data is read from the source, it is currated through the medallion structure of bronze to gold. The ETL flow is built with Delta Live Tables (DLT) of which you can easily monitor the health of your incoming data by setting expectations. This ETL flow should be encapsulated by a job, which can be set to a schedule.

In the same job but as another task, should the daily prediction be performed. The gold standard data is sent to the trained machine learning (ML) model, which makes daily predictions. The ML model should have a few requirements. One, it should not brake for new data or if new columns are sent to it. Two, the health of the incoming data and the model should be automatically monitored, and alerts should be sent as soon as a data or model drift occurs. The drifts are monitored with something called Evidently, which is open source. The predictions are saved in a table, and the insights from the table are visualized in a dashboard that updates daily. 

This should be built in Databricks, utilizing testing and a CI/CD approach with three environments (DEV, TEST, PROD), be written in PySpark, and make use of a proper IDE. Visual Studio Code just recently launched a Databricks extension that I wanted to try out. In addition, I wanted to try something called Sphinx and Autodocstring, which are used for documentation. Sphinx produces a searchable web-based interface of your modules and functions, and Autodocstring produces a doc template for each function while coding. Explorative data analysis should be excluded.

## What was actually built
The data used for the project is the AirBnb San Fransisco. 

[path](docs/build/html/index.html)

