import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import logging

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class NBADataScraper:
    def __init__(self):
        self.base_url = "https://www.basketball-reference.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
    def get_page_content(self, url):
        """Fetch page content with error handling and rate limiting"""
        try:
            logging.info(f"Fetching URL: {url}")
            time.sleep(3)  # Rate limiting
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error for {url}: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching {url}: {str(e)}")
            return None

    def get_mvp_winner(self, year):
        """Extract MVP winner for a given season"""
        url = f"{self.base_url}/awards/awards_{year}.html"
        soup = self.get_page_content(url)
        if not soup:
            logging.error(f"Failed to get MVP page for {year}")
            return None
            
        try:
            mvp_table = soup.find('table', {'id': 'mvp'})
            if not mvp_table:
                logging.error(f"Could not find MVP table for {year}")
                return None
                
            # Get the first row (winner)
            winner_row = mvp_table.find('tbody').find('tr')
            if not winner_row:
                logging.error(f"Could not find MVP winner row for {year}")
                return None
                
            player_name = winner_row.find('td', {'data-stat': 'player'}).text.strip()
            logging.info(f"Found MVP winner for {year}: {player_name}")
            return player_name
        except Exception as e:
            logging.error(f"Error extracting MVP winner for {year}: {str(e)}")
            return None

    def get_basic_stats(self, year):
        """Extract basic statistics for a given season"""
        url = f"{self.base_url}/leagues/NBA_{year}_per_game.html"
        soup = self.get_page_content(url)
        if not soup:
            logging.error(f"Failed to get basic stats page for {year}")
            return None
            
        try:
            stats_table = soup.find('table', {'id': 'per_game_stats'})
            if not stats_table:
                logging.error(f"Could not find basic stats table for {year}")
                return None
                
            # Convert table to DataFrame
            df = pd.read_html(str(stats_table))[0]
            
            # Print available columns for debugging
            logging.info(f"Available columns for {year}: {df.columns.tolist()}")
            
            # Clean up the DataFrame
            df = df[df['Player'].notna()]  # Remove rows where Player is NaN
            df = df[~df['Player'].str.contains('Player')]  # Remove header rows
            
            # Map column names to handle different naming conventions
            column_mapping = {
                'Tm': 'Team',
                'Pos': 'Position',
                'G': 'Games',
                'MP': 'Minutes',
                'PTS': 'Points',
                'TRB': 'Rebounds',
                'AST': 'Assists',
                'STL': 'Steals',
                'BLK': 'Blocks',
                'TOV': 'Turnovers',
                'FG%': 'FG_Pct',
                '3P%': '3P_Pct',
                'FT%': 'FT_Pct'
            }
            
            # Rename columns if they exist
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # Select relevant columns (using new names)
            columns = ['Player', 'Team', 'Position', 'Season', 'Games', 'Minutes', 'Points', 
                      'Rebounds', 'Assists', 'Steals', 'Blocks', 'Turnovers', 
                      'FG_Pct', '3P_Pct', 'FT_Pct']
            
            # Only select columns that exist in the DataFrame
            available_columns = [col for col in columns if col in df.columns]
            if not available_columns:
                logging.error(f"No matching columns found for {year}")
                return None
                
            df = df[available_columns]
            logging.info(f"Successfully extracted basic stats for {year}")
            return df
        except Exception as e:
            logging.error(f"Error extracting basic stats for {year}: {str(e)}")
            return None

    def get_advanced_stats(self, year):
        """Extract advanced statistics for a given season"""
        url = f"{self.base_url}/leagues/NBA_{year}_advanced.html"
        soup = self.get_page_content(url)
        if not soup:
            logging.error(f"Failed to get advanced stats page for {year}")
            return None
            
        try:
            # Look for the table with class 'stats_table'
            stats_table = soup.find('table', {'class': 'stats_table'})
            if not stats_table:
                logging.error(f"Could not find advanced stats table for {year}")
                return None
                
            # Convert table to DataFrame
            df = pd.read_html(str(stats_table))[0]
            
            # Print available columns for debugging
            logging.info(f"Available advanced columns for {year}: {df.columns.tolist()}")
            
            # Clean up the DataFrame
            df = df[df['Player'].notna()]  # Remove rows where Player is NaN
            df = df[~df['Player'].str.contains('Player')]  # Remove header rows
            
            # Map column names to handle different naming conventions
            column_mapping = {
                'PER': 'Player_Efficiency_Rating',
                'WS': 'Win_Shares',
                'BPM': 'Box_Plus_Minus',
                'USG%': 'Usage_Rate',
                'VORP': 'Value_Over_Replacement',
                'WS/48': 'Win_Shares_Per_48'
            }
            
            # Rename columns if they exist
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # Select relevant columns (using new names)
            columns = ['Player', 'Player_Efficiency_Rating', 'Win_Shares', 
                      'Box_Plus_Minus', 'Usage_Rate', 'Value_Over_Replacement', 
                      'Win_Shares_Per_48']
            
            # Only select columns that exist in the DataFrame
            available_columns = [col for col in columns if col in df.columns]
            if not available_columns:
                logging.error(f"No matching advanced columns found for {year}")
                return None
                
            df = df[available_columns]
            logging.info(f"Successfully extracted advanced stats for {year}")
            return df
        except Exception as e:
            logging.error(f"Error extracting advanced stats for {year}: {str(e)}")
            return None

    def scrape_season(self, year):
        """Scrape all data for a given season"""
        logging.info(f"Starting to scrape data for {year} season...")
        
        # Get MVP winner
        mvp_winner = self.get_mvp_winner(year)
        if mvp_winner is None:
            logging.error(f"Failed to get MVP winner for {year}")
            return None
        
        # Get basic stats
        basic_stats = self.get_basic_stats(year)
        if basic_stats is None:
            logging.error(f"Failed to get basic stats for {year}")
            return None
            
        # Get advanced stats
        advanced_stats = self.get_advanced_stats(year)
        if advanced_stats is None:
            logging.error(f"Failed to get advanced stats for {year}")
            return None
            
        try:
            # Merge basic and advanced stats
            merged_stats = pd.merge(basic_stats, advanced_stats, on='Player', how='left')
            
            # Add MVP column
            merged_stats['MVP'] = merged_stats['Player'].apply(lambda x: 1 if x == mvp_winner else 0)
            
            # Add Season column
            merged_stats['Season'] = year
            
            logging.info(f"Successfully merged all data for {year}")
            return merged_stats
        except Exception as e:
            logging.error(f"Error merging data for {year}: {str(e)}")
            return None

    def scrape_all_seasons(self, start_year=1981, end_year=2024):
        """Scrape data for all seasons in the given range"""
        all_data = []
        
        for year in tqdm(range(start_year, end_year + 1), desc="Scraping seasons"):
            season_data = self.scrape_season(year)
            if season_data is not None:
                all_data.append(season_data)
                logging.info(f"Successfully scraped {year} season")
            else:
                logging.error(f"Failed to scrape {year} season")
                
        if not all_data:
            logging.error("No data was collected!")
            return None
            
        # Combine all seasons
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Clean up the final DataFrame
        final_df = final_df.fillna(0)  # Fill missing values with 0
        
        # Ensure all numeric columns are float
        numeric_columns = ['Minutes', 'Points', 'Rebounds', 'Assists', 'Steals', 
                         'Blocks', 'Turnovers', 'FG_Pct', '3P_Pct', 'FT_Pct', 
                         'Player_Efficiency_Rating', 'Win_Shares', 'Box_Plus_Minus', 
                         'Usage_Rate', 'Value_Over_Replacement', 'Win_Shares_Per_48']
        
        for col in numeric_columns:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
        
        return final_df

def main():
    scraper = NBADataScraper()
    final_df = scraper.scrape_all_seasons()
    
    if final_df is not None:
        # Reorder columns to put Season first
        cols = final_df.columns.tolist()
        cols.remove('Season')
        cols = ['Season'] + cols
        final_df = final_df[cols]
        
        # Save to CSV
        output_file = 'nba_mvp_data.csv'
        final_df.to_csv(output_file, index=False)
        logging.info(f"Data successfully saved to {output_file}")
        
        # Print some basic statistics about the dataset
        logging.info(f"\nDataset Statistics:")
        logging.info(f"Total number of player-seasons: {len(final_df)}")
        logging.info(f"Number of MVP winners: {final_df['MVP'].sum()}")
        logging.info(f"Seasons covered: {final_df['Season'].min()} to {final_df['Season'].max()}")
    else:
        logging.error("Failed to create the dataset")

if __name__ == "__main__":
    main() 