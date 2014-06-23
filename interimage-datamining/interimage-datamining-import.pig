/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

/**
 * A Pig script that defines the data mining package UDFs.
 * @author Victor Quirita, Rodrigo Ferreira
 */

--Eval UDFs
DEFINE II_Membership br.puc_rio.ele.lvc.interimage.datamining.udf.Membership('https://s3.amazonaws.com/interimage2/resources/fuzzysets.ser');
DEFINE II_BayesClassifier br.puc_rio.ele.lvc.interimage.datamining.udf.BayesClassifier('https://s3.amazonaws.com/interimage2/resources/training-data.csv');
DEFINE II_DecisionTreeClassifier br.puc_rio.ele.lvc.interimage.datamining.udf.DecisionTreeClassifier('https://s3.amazonaws.com/interimage2/resources/training-data.csv');
DEFINE II_RandomForestClassifier br.puc_rio.ele.lvc.interimage.datamining.udf.RandomForestClassifier('https://s3.amazonaws.com/interimage2/resources/training-data.csv');
DEFINE II_SVMClassifier br.puc_rio.ele.lvc.interimage.datamining.udf.SVMClassifier('https://s3.amazonaws.com/interimage2/resources/training-data.csv');

--Filter UDFs

--Special UDFs
