# !/usr/bin/python

import os
import re
import logging
import shutil
import time
from logging.config import fileConfig
import subprocess

fileConfig('logging.ini')
log = logging.getLogger()

VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.m4v', '.m2ts')

"""
Custom Error Definitions
"""
class CouldNotCreateFile(Exception):
    pass

class MediaRootDoesNotExist(Exception):
    pass

class FailedMovingFile(Exception):
    pass

class FailedTranscoding(Exception):
    pass

"""
The file transcode log keeps track of the files that we have transcoded successfully
It is a simple file that appends the new path of a successfull transcode AFTER the entire
procedure is completed.  This is a simple file so we can remove entries by hand if failures
do occur in the log, however all protections are in place so that any hard failures of this
procedure do not break the movie files themselves.
The Batch Transcoder will update this log as necessary
"""
class FileTranscodeLog:
    def __init__(self):
        self.transcoded_paths = set()
        self.transcode_log_fname = 'transcoded-paths.log'
        rem_newline_regex = re.compile(r"\n$", re.IGNORECASE)
        try:
            with open(self.transcode_log_fname, 'r') as f:
                log.info('Opened file {}'.format(self.transcode_log_fname))
                for line in f.readlines():
                    self.transcoded_paths.add(rem_newline_regex.sub('', line))
                f.close()
        except IOError:
            try:
                log.warning('File [{}] doesn\'t exist - will try to create...'.format(self.transcode_log_fname))
                f = open(self.transcode_log_fname, 'w')
                log.info('File [{}] created successfully'.format(self.transcode_log_fname))
                f.close()
            except:
                error_message = 'Could not create file {}'.format(self.transcode_log_fname)
                log.error(error_message)
                raise CouldNotCreateFile(error_message)

    def add_success(self, fPath):
        """
        We need to write to both the file and the data representation in memory
        """
        with open(self.transcode_log_fname, 'a') as f:
            log.info('Adding {} to transcoded_paths file'.format(fPath))
            f.write(fPath + '\n')
            f.close()
            self.transcoded_paths.add(fPath)

class Transcoder:
    def __init__(self):
        # handbrakecli -Z "Apple 1080p60 Surround" -i test-dirs/movies\ 3/Beck\ -\ Sea\ Change\ \(DTS-HDMA\ 24-192kHz\ 5.1\ \(45sec\).mkv -o ./test-beck.mkv


    def transcode(self, src_file, dest_file):
        transcode_proc = subprocess.run([
            'handbrakecli',
            '-Z "Apple 1080p60 Surround"'
            '-i {}'.format(src_file)
            '-o {}'.format(dest_file)
        ])
        print('should not happen immpediately')

class BatchProcessor:
    def __init__(self, media_root=None, batch_count=10, dry_run=True):
        # Only the most basic job details/statistics
        self.job_status_log = open('./job-logs/job-' + str(int(time.time())) + '.log', 'a')
        # Stores the successfull transcodes
        self.transcode_log = FileTranscodeLog()
        # Misc options
        self.batch_count = batch_count
        self.processed_files = 0
        # self.current_job = None
        self.dry_run = dry_run

        if os.path.exists(media_root):
            self.media_root = media_root
        else:
            error_message = 'Media Root Folder does not exist - cannot proceed! '.format(media_root)
            log.error(error_message)
            raise MediaRootDoesNotExist(media_root)

        self.processing_dir = '.processing'
        if not os.path.exists(self.processing_dir):
            os.makedirs(self.processing_dir)

        self.paths_to_process = list()
        self.find_movie_paths()
        sorted(self.paths_to_process, key=lambda k: k['orig_fsize'], reverse=True)
        # self.prune_good_movies()

        self.start_processing_loop()

        self.job_status_log.close()

    def start_processing_loop(self):
        self.output_starting_stats()
        for jidx, job in enumerate(self.paths_to_process):
            if self.processed_files >= self.batch_count:
                self.job_status_log.write('Reached batch limit - exiting \n')
                break
            try:
                self.job_status_log.write('\nStarting Job {}/{}\n'.format(jidx + 1, self.batch_count))
                self.job_status_log.write('source_file: {}\nsize: {}\n'.format(job['source_file'], job['orig_fsize']))
                self.move_file_to_processing_dir(job)
                time.sleep(.3)
                self.move_file_back_to_media_root(job)
            except FailedMovingFile as move_error:
                continue
            except FailedTranscoding as transcoding_error:
                continue

    def output_starting_stats(self):
        self.job_status_log.write('********************************************************************************************\n')
        self.job_status_log.write('Processing Loop Started:\n')
        self.job_status_log.write('media_root = {}\n'.format(self.media_root))
        self.job_status_log.write('dry_run = {}\n'.format(self.dry_run))
        self.job_status_log.write('batch_count = {}\n'.format(self.batch_count))
        self.job_status_log.write('found {} video files in media root\n'.format(len(self.paths_to_process)))
        self.job_status_log.write('found {} video files in processed log - these won\'t be processed\n'.format(len(self.transcode_log.transcoded_paths)))
        self.job_status_log.write('********************************************************************************************\n')

    def find_movie_paths(self):
        log.info('Starting directory walk through {}'.format(self.media_root))
        for movie_dir, subdirs, files in os.walk(self.media_root, topdown=False):
            for file in files:
                if file.endswith(VIDEO_EXTENSIONS):
                    file_path = os.path.join(movie_dir, file)
                    if os.path.isfile(file_path):
                        if not file_path in self.transcode_log.transcoded_paths and not file.startswith('temp-'):
                            log.info('Adding job to processing queue - {}'.format(file_path))
                            self.paths_to_process.append(dict(
                                # original source file
                                source_file=file_path,
                                # File name to use for processing
                                processing_file=os.path.join(self.processing_dir, file),
                                transcoder_output=os.path.join(self.processing_dir, 'processed-' + file),
                                # Temp source file is a temp file we copy the processed movie TO
                                # so we don't overwrite the original file while we are moving which
                                # could make network errors fuck up our movies
                                temp_source_file=os.path.join(movie_dir, 'temp-' + file),
                                # Source file size, used to prioritize jobs
                                orig_fsize=os.path.getsize(file_path)
                            ))
                        else:
                            log.warning('skipping queue - {}'.format(file_path))
                    else:
                        log.error('DirWalk Error - Skipping - {}'.format(file_path))

    def move_file_to_processing_dir(self, job):
        try:
            shutil.copy2(job['source_file'], job['processing_file'])
            self.job_status_log.write('MOVED {} to processing directory\n'.format(job['source_file']))
        except Exception as e:
            self.job_status_log.write('ERROR Moving {} to processing directory\n'.format(job['source_file']))
            log.error(e)
            raise FailedMovingFile('Failed to move file to processing directory')

    def move_file_back_to_media_root(self, job):
        try:
            # TODO: change this to move the transcoder_output file
            shutil.copy2(job['processing_file'], job['temp_source_file'])
        except Exception as e:
            log.error(e)
        finally:
            os.remove(job['processing_file'])

    def rename_processed_file_in_media_root(self, job):
        os.remove(job['source_file']) # remove the original
        os.rename(job['temp_source_file'], )

# t = BatchProcessor(media_root='./test-dirs', dry_run=True, batch_count=1)
