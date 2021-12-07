import twint
import pandas as pd
from tqdm import tqdm
import nest_asyncio
import argparse

nest_asyncio.apply()

def init_config(args):
    c = twint.Config()

    # configure Twint Configuration object
    c.Search = args.query       # query searched for
    c.Since = args.since_date   # the start date
    c.Limit = args.max_count   # max number of tweets
    c.Hide_output = True
    c.Pandas = True     # create a pandas dataframe
    c.Store_pandas = True # store as pandas object
    c.Near = args.states      # crawl tweets if there were written near given state

    return c

def get_df(config):

    # run service
    twint.run.Search(config)

    # get pandas dataframe object for tweets crawled
    df = twint.storage.panda.Tweets_df

    # filter the number of the rows
    # since it works wrong in Twint's code
    df = df[:config.Limit]

    #filter the columns needed
    df = df.filter(items=['username', 'tweet', 'link', 'date', 'language', 'near'])

    return df

def main (args):

    # initialize config class using args
    c = init_config(args)

    # create an empty dataframe
    dataset = pd.DataFrame()

    # for each state in states
    for s in tqdm(args.states):
        # create a dataframe
        df = get_df(c)

        #append the dataframe end of the existing dataframe
        dataset = dataset.append(df, ignore_index=True)

    # rite dataframe to a csv file
    dataset.to_csv(args.output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="tweet_crawler", 
                                     description='Tweet crawler using queries',
                                     usage="python3 %(prog)s [OPTIONS]")

    parser.add_argument('-q', '--query', help="a query string to search", required=True)
    parser.add_argument('-d', '--since_date', help='the very first date for tweets [YYYY-MM-DD]', required=True)
    parser.add_argument('-c', '--max_count', type=int, default=100, help='maximum number of tweets for each state', required=True)
    parser.add_argument('-s', '--states', nargs='*', help='crawl teets near the state in states', required=True)
    parser.add_argument('-o', '--output', default='out.csv', help='output file name')

    # parse arguments
    args = parser.parse_args()
    
    main(args)
