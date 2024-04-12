# Address Extraction

## Explanation and Reasoning
For this challenge, I developed a Python program that extracts valid addresses from a list of company websites. I chose Python due to its versatility and the availability of robust libraries for web data analysis and dataset manipulation, such as BeautifulSoup for HTML analysis, pandas for working with datasets, and requests for making HTTP requests.

## Correctness
The program adheres to the specified requirements, correctly extracting addresses from the analyzed web pages and storing them in an appropriate format.

## Robustness
The program handles common HTTP errors and text decoding situations through exception handling for HTTP requests or text decoding.

## Code Quality
The code is well-organized, easy to read, and maintainable. I used clear variable names and comments to explain the logic of each part of the code.

## Creativity and Going the Extra Mile
In the current version of the program, I decided not to add functionality for identifying the country from the extracted address. This choice was based on a combination of the following factors:
1. **Time Constraint**
   - I wanted to deliver the solution as quickly as possible, and adding this functionality would have required additional development time.
2. **Hardware Restrictions**
   - My work laptop does not allow the use of any APIs on the market, and my personal laptop was not powerful enough to run the necessary code. However, I plan to acquire a more capable laptop in the near future, which won’t pose a problem for the tasks related to the open position at Veridion.
