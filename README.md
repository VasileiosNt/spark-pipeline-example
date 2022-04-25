# Description
A simple spark pipeline that transforms supplier's data (source) into a target dataset. The source data are Pre-processed, Normalized, simple Feature Extraction and lastly Integrated.

##### Pre-processing
- load the data into a dataset and transform the supplier data to achieve the same granularity as the target data.

##### Normalization 
- “BodyColorText”: is translated into English and to match target values in target attribute “color”
- “MakeText”:  normalised to match target values in target attribute “make”

##### Extraction 
- The value of the consumption from the supplier attribute “ConsumptionTotalText” into an attribute called: “ex-tracted-value-ConsumptionTotalText”
- The unit of the consumption from the supplier attribute “ConsumptionTotalText” into an attribute called: “ex-tracted-unit-ConsumptionTotalText”

##### Integration
- Integrate source to target.


# Get Started 

To run the pipeline run the following under the `app` folder:
 - `make spark-run` (Initializes the spark cluster)
 - `make poetry` (installs the poetry package manager)
 - `make install` (installs the dependencies)
 - `make run-job` (start the spark pipeline)
 - `make tests` (runs the tests)


#### Notes
- For the translation process it calls a free service (Takes up to 20 minutes), if you would like to skip it comment the lines `52-58` at `app/utils/source_normalizer`), the color column will be written as "Other".
- Needs $JAVA_HOME to point to JAVA 8.

