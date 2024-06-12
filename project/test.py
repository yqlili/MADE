import unittest
import os
from pipeline import run_pipline

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        # Define the download directory relative to the current folder
        download_dir = "data/"
        # Get the target directory to store data from the environment variable
        target_folder_path = os.environ.get('TARGET_DIR')
        # Concatenate the current folder path and download directory
        full_download_dir = os.path.join(target_folder_path, download_dir)

        # Set up environment variables and any initial configurations
        self.target_folder_path = full_download_dir

    def tearDown(self):
        # Clean up the environment after tests
        if os.path.exists(self.target_folder_path):
            for root, dirs, files in os.walk(self.target_folder_path, topdown=False):
                for name in files:
                    if name.endswith('.csv') or name.endswith('.sqlite'):
                        os.remove(os.path.join(root, name))
                # # Remove empty directories if needed, except the base data folder
                # for name in dirs:
                #     dir_path = os.path.join(root, name)
                #     if os.path.isdir(dir_path) and not os.listdir(dir_path):
                #         os.rmdir(dir_path)

    def test_run_pipeline(self):

        # Run the pipeline
        run_pipline()

        # Check that at least one file that starts with "Emissions" exists
        emissions_files = [f for f in os.listdir(self.target_folder_path) if f.startswith('Emissions')]
        self.assertTrue(len(emissions_files) > 0, "Emissions CSV file was not created")

        # Check that at least one file that starts with "Vehicle" exists
        vehicle_files = [f for f in os.listdir(self.target_folder_path) if f.startswith('Vehicle')]
        self.assertTrue(len(vehicle_files) > 0, "Vehicle CSV file was not created")
        
if __name__ == '__main__':
    unittest.main()
