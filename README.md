<h2 align="center">Car Carsh Analysis {BCG GAMMA Case Study}</h3>

<div align="center">

[![Status](https://img.shields.io/badge/status-active-success.svg)]()
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/kylelobo/The-Documentation-Compendium.svg)](https://github.com/kylelobo/The-Documentation-Compendium/pulls)

</div>

---
<p align="center"> Develop a modular application uses spark-submit to provide results for given tasks.
    <br> 
</p>

## 📝 Table of Contents

- [📝 Table of Contents](#-table-of-contents)
- [🧐 About ](#-about-)
- [Project File Structure ](#project-file-structure-)
- [🏁 Getting Started ](#-getting-started-)
  - [Prerequisites](#prerequisites)
  - [Installing](#installing)
- [🔧 Running the tests ](#-running-the-tests-)
  - [Break down into end to end tests](#break-down-into-end-to-end-tests)
  - [And coding style tests](#and-coding-style-tests)
- [🎈 Usage ](#-usage-)
- [🚀 Deployment ](#-deployment-)
- [⛏️ Built Using ](#️-built-using-)
- [✍️ Authors ](#️-authors-)
- [🎉 Acknowledgements ](#-acknowledgements-)

## 🧐 About <a name = "about"></a>

<h4> Use 6 csv files in the raw_folder and develop your approach to perform below analytics.</h4>

<h4>Requirements</h4>
  Application should perform below analysis and store the results for each analysis in the destination folder.
  <ol>
    <li>Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?</li>
    <li>Analysis 2: How many two wheelers are booked for crashes?</li> 
    <li>Analysis 3: Which state has highest number of accidents in which females are involved? </li>
    <li>Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death.</li>
    <li>Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.</li>
    <li>Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code).</li>
    <li>Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.</li><li>Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data).</li>

## Project File Structure <a name = "project"></a>

The basic project structure is shown as below:
```
CarCrashAnalysis-BCG
├─ .gitignore
├─ README.md
├─ config
│  └─ config.json
├─ jobs
│  ├─ __init__.py
│  ├─ job.py
│  ├─ jobbuilder.py
│  ├─ loader.py
│  ├─ logger.py
│  ├─ settings.py
│  └─ utils.py
├─ notebooks
│  └─ workbook.ipynb
├─ requirements.txt
├─ resources
│  ├─ logs
│  ├─ processed
│  │  ├─ Question_1
│  │  │  ├─ ._SUCCESS.crc
│  │  │  ├─ .part-00000-cfae5228-be9e-456b-82a7-1a8be3e75b47-c000.csv.crc
│  │  │  ├─ _SUCCESS
│  │  │  └─ part-00000-cfae5228-be9e-456b-82a7-1a8be3e75b47-c000.csv
│  │  ├─ Question_2
│  │  │  ├─ ._SUCCESS.crc
│  │  │  ├─ .part-00000-f02dac60-a57f-4384-a945-87965281456b-c000.csv.crc
│  │  │  ├─ _SUCCESS
│  │  │  └─ part-00000-f02dac60-a57f-4384-a945-87965281456b-c000.csv
│  │  ├─ Question_3
│  │  │  ├─ ._SUCCESS.crc
│  │  │  ├─ .part-00000-cad9913b-e1eb-4006-b8b2-dce7572f4a7e-c000.csv.crc
│  │  │  ├─ _SUCCESS
│  │  │  └─ part-00000-cad9913b-e1eb-4006-b8b2-dce7572f4a7e-c000.csv
│  │  ├─ Question_4
│  │  │  ├─ ._SUCCESS.crc
│  │  │  ├─ .part-00000-fef5dd18-6b02-4ad9-9818-1b56d4cb72e2-c000.csv.crc
│  │  │  ├─ _SUCCESS
│  │  │  └─ part-00000-fef5dd18-6b02-4ad9-9818-1b56d4cb72e2-c000.csv
│  │  ├─ Question_5
│  │  │  ├─ ._SUCCESS.crc
│  │  │  ├─ .part-00000-bb2305a2-89b6-4244-a48c-078be6506df1-c000.csv.crc
│  │  │  ├─ _SUCCESS
│  │  │  └─ part-00000-bb2305a2-89b6-4244-a48c-078be6506df1-c000.csv
│  │  ├─ Question_6
│  │  │  ├─ ._SUCCESS.crc
│  │  │  ├─ .part-00000-7c3fae01-564b-4929-a972-ea92a2aca3ef-c000.csv.crc
│  │  │  ├─ _SUCCESS
│  │  │  └─ part-00000-7c3fae01-564b-4929-a972-ea92a2aca3ef-c000.csv
│  │  ├─ Question_7
│  │  │  ├─ ._SUCCESS.crc
│  │  │  ├─ .part-00000-9292f6e7-c9fc-4314-9521-16cf969ee0da-c000.csv.crc
│  │  │  ├─ _SUCCESS
│  │  │  └─ part-00000-9292f6e7-c9fc-4314-9521-16cf969ee0da-c000.csv
│  │  └─ Question_8
│  │     ├─ ._SUCCESS.crc
│  │     ├─ .part-00000-c7f26cb6-920e-478e-a2de-a5cded5eef32-c000.csv.crc
│  │     ├─ _SUCCESS
│  │     └─ part-00000-c7f26cb6-920e-478e-a2de-a5cded5eef32-c000.csv
│  └─ raw
│     ├─ Charges_use.csv
│     ├─ Damages_use.csv
│     ├─ Endorse_use.csv
│     ├─ Primary_Person_use.csv
│     ├─ Restrict_use.csv
│     └─ Units_use.csv
├─ runner.py
└─ tests
   └─ __init__.py

```

## 🏁 Getting Started <a name = "getting_started"></a>

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them.

```
Give examples
```

### Installing

A step by step series of examples that tell you how to get a development env running.

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo.

## 🔧 Running the tests <a name = "tests"></a>

Explain how to run the automated tests for this system.

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## 🎈 Usage <a name="usage"></a>

Add notes about how to use the system.

## 🚀 Deployment <a name = "deployment"></a>

Add additional notes about how to deploy this on a live system.

## ⛏️ Built Using <a name = "built_using"></a>

- [MongoDB](https://www.mongodb.com/) - Database
- [Express](https://expressjs.com/) - Server Framework
- [VueJs](https://vuejs.org/) - Web Framework
- [NodeJs](https://nodejs.org/en/) - Server Environment

## ✍️ Authors <a name = "authors"></a>

- [@kylelobo](https://github.com/kylelobo) - Idea & Initial work

See also the list of [contributors](https://github.com/kylelobo/The-Documentation-Compendium/contributors) who participated in this project.

## 🎉 Acknowledgements <a name = "acknowledgement"></a>

- Hat tip to anyone whose code was used
- Inspiration
- References