import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import time

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
            
            # Add Season column
            merged_stats['Season'] = year
            
            logging.info(f"Successfully merged all data for {year}")
            return merged_stats
        except Exception as e:
            logging.error(f"Error merging data for {year}: {str(e)}")
            return None

def main():
    scraper = NBADataScraper()
    year = 2025  # Only scrape 2024-2025 season
    season_data = scraper.scrape_season(year)
    
    if season_data is not None:
        # Reorder columns to put Season first
        cols = season_data.columns.tolist()
        cols.remove('Season')
        cols = ['Season'] + cols
        season_data = season_data[cols]
        
        # Clean up the DataFrame
        season_data = season_data.fillna(0)  # Fill missing values with 0
        
        # Ensure all numeric columns are float
        numeric_columns = ['Minutes', 'Points', 'Rebounds', 'Assists', 'Steals', 
                         'Blocks', 'Turnovers', 'FG_Pct', '3P_Pct', 'FT_Pct', 
                         'Player_Efficiency_Rating', 'Win_Shares', 'Box_Plus_Minus', 
                         'Usage_Rate', 'Value_Over_Replacement', 'Win_Shares_Per_48']
        
        for col in numeric_columns:
            if col in season_data.columns:
                season_data[col] = pd.to_numeric(season_data[col], errors='coerce')
        
        # Save to CSV
        output_file = 'nba_2025_season.csv'
        season_data.to_csv(output_file, index=False)
        logging.info(f"Data successfully saved to {output_file}")
        
        # Print some basic statistics about the dataset
        logging.info(f"\nDataset Statistics:")
        logging.info(f"Total number of players: {len(season_data)}")
        logging.info(f"Season: {year}")
    else:
        logging.error("Failed to create the dataset")

if __name__ == "__main__":
    main() 