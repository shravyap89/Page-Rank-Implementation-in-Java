Sarah Talty
Sravya
CIS (4930/6930) / Project I
report.txt


------------LOCAL HADOOP SETUP------------
This was probably what we had the most difficulty setting up. Both of us had first tried to setup Hadoop on our Windows machines. Our first attempt was to use Eclipse 
with a Hadoop-1.0.3 plugin for debugging and running test samples for each job. However, neither of us were able to successfully install and connect Hadoop to 
Eclipse through a Cygwin installation. We tried for about a week to get our Windows local installation to work, but we ended up simply debugging our code with Eclipse,
installing Hadoop on a student access Linux machine, and using remote access to upload our data and run tests from home.  We did have problems with understanding how to
get the Hadoop libraries to be recognizd, and after asking for  help found that we needed a Java path to the Hadoop core.


--------------TASKS DIVISION--------------
With the Project's original 4-job structure and differing levels of Java knowledge, we had decide the following job breakdown: Sarah - 1,3; Sravya - 2,4.  However, with
a lot of the project description changing we ended up with a general set of Sarah - Redlink removal, outlink mapping, PageRank calculation iterations ; Sravya - Page Count,
PageRank sorting.  Because it took a while to understand how Hadoop works we did a lot of pencil-paper drawings and outlines of each other's 
jobs, helping each other with debugging, and providing each other with resources and pieces of code to help out.

OPTIMIZATION CHANGES:
-To help with project structure opimization we decided to change the job flow in the Project Description and make Page Count our second job in order to reference the
results in later jobs.

----------------JOB #1:RedLink removal--------------------------------------------------------------------------------------------------------------------------------
Sarah-
This was a little tricky to understand.  I needed help to clarify on what RedLinks were and after talking to Kun Li, an explanation from Dr. Daisy, and doing some
research I learned that the major part of this job was figuring out how to specify a page exists and, if it doesn't, to exclude them from outlink lists. I knew that
iterating throught the input more than once would be extremely inefficient, so in the end, for the Mapper I first used a "$" to flag Titles by emitting <Title, $> 
of found pages and then emitting <Outlink, Title>.  In the Reducer, I used the flag to skip over links without existing pages and then, for those with "$" I emitted
<Title, Outlink>.
----------------JOB #2:PageCount--------------------------------------------------------------------------------------------------------------------------------------
Sravya

Steps required to code Job 3: CALCULATING TOTAL NUMBER OF PAGES
This job requires the total number of pages that is value "N" after removing red  links and generating wikilinks file.

Mapper: Mapper takes as input the output of Job2 which is basically in the format:
PageA PageB Page C
PageB PageC
i.e Page name followed by the links.

Mapper implements function of "Tokeniser". Here the token is " "(Space). Because after page name or title of page there is space and after that all the links follow.
Hence, as soon as the first token is encountered the string before that is emitted as value.Also, we take a commong key called "N" and values for the keys is all
the title pages enlisted in the inlink-graph generated from given data set.

----------------JOB #3:OutlinkMapping---------------------------------------------------------------------------------------------------------------------------------
Sarah-
After taking in the output from our Job1 and retrieving the value of N from our Job2 is constructed an outlink map by having this job's Mapper emit <pageTitle, 1/N outlinks>
(pageTitle from values and outlinks from keys).  The difficult part here was learning how to retrieve N from Job2 and passing it to the mapper.  I found out how to do this
through using JobConf.set(String name, value).  This job's mapper is where I ignored key/value pairs that didn't have the flag "$" that I inserted in Job1 (for some reason
it didn't work when trying to ignore the unflagged links in Job#1.  Because I decided to do the flag check here, I adjusted the Job2 PageCount code to also ignore key/value
pairs without the "$" flag when counting number of pages.
----------------JOB #4:PageRankCalculation----------------------------------------------------------------------------------------------------------------------------
Sarah-
The input for this job is the Outlink output from Job3.  I was confused on how to properly give each node their own degree and page rank for the calculation.  After 
talking to Kun, I was better able to better understand the given algorithm concept on the Lecture Slides and Project Description.  For the mapper, I first emitted
<pageTitle, outlink list> and then also emited <outlink, page's PR/degree>.  The page's PR comes from either the initial 1/N insertion done in Jo3 or a previous itieration's
calculation. The degree comes from the length of the outlink list.  Each page in the Reducer now has all the incominglink's PR/degree and all that is needed is to multiply 
by 0.85 and sum them all with the given constant.
----------------JOB #5:PageRankReordering-----------------------------------------------------------------------------------------------------------------------------
Sravya-

This job essentially is to output the Page title along with its calculated rank after  power iteration and arrange them in Desecending order.

Mapper: Mapper takes as input the output of Job4 which is in the format:
PageA 1.45 PageB PageC
PageB 1.2  PageC
i.e Page name followed by Pagerank value and Links.

Mapper implements function of "Tokeniser". Here the token is " "(Space) Because after page name or title of page there is space and after that the Pagerank value 
appears and then the links. Hence, as soon as first token is encountered the string before that is stored in emitted a key value. Now, "Parsedouble" function is 
used to extract the double number from string wherein again "next tokensier" is used to produce input string for parsedouble. This extracts double number from string.
Now,mapper runs and extracts double number and mutliplies it with -1 and emits as a key for the value initially taken from the first tokeniser function.Also also those Pages which have page rank greater than 5/N need to be passed on to reducer.Therefore, a "if" statement is used which compares 
page rank with 5/N where N is obtained from Job2.

Reducer: Reducer takes as input the key and values. Now, here the reducers will automatically sort the key field in ascending order while receiving the key values from Mapper. 
Hence, We can again mutiply the  key value by -1 to obtain the intial page rank and emit it along with their value which do not undego any change in the reducer.
In this way we can sort the Page rank in their descending order.

Instead of using writable comparator, we have used a simpler logic to implement the desecending order functionality in the Job.

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
What have we learnt from the project:
In this project we have learnt how to implement Page rank to find the most important Wikipedia pages using AWS 
Elastic MapReduce.(EMR)

In the first phase of project we have use MapReduce functionality of Hadoop which enable us to simulatenously
processing of Map and Reduction operation. We have Map Reduce functionality in Job1: To generate wikilink graph by
removing redlinks, Job2: To calculate the total number of pages,Job3:Outlinkink Mapping , Job4:Page rank 
calculation, Job5:Page Rank ordering.
The specific functionality of each Mapper and Reducer in each phase has been mentioned above.
We have been able to associate theoritical knowledge and implement pratically how do the mapper and reducers
function.

In the second phase of prokect we have used Amazaon Web services to process the 43GB data set which had been given
to us.
This phase included creating job jar, hands on experience with EC2 and S3 definitions for Data processing.
We could run our program on local machine using Hadoop. But implementing the same on AWS we faced a few problems.
Our attempt to open an S3 file using java code was unsucessful at first. But with proper implementation of Code we
could run our code on AWS and procure results for all steps.
We learnt how AWS can efficiently process large amount of Data.

