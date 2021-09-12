import boto3
import botocore.exceptions
import logging
import os
import pickle
import pandas as pd
import pytz
from datetime import datetime
from flask import Flask
from flask_restful import Resource, Api, reqparse, abort
from flask_cors import CORS

DEST = '/Users/feixie/Code/image_loader/data/'
MAX_EPOCH = datetime.now().timestamp()
MIN_EPOCH = 0
utc=pytz.UTC

app = Flask(__name__)
CORS(app)
api = Api(app)

# for request data validation
parser = reqparse.RequestParser()
parser.add_argument("from_time_epoch", default=MIN_EPOCH)
parser.add_argument("to_time_epoch", default=MAX_EPOCH)

s3 = boto3.resource('s3')
ceres_bucket = s3.Bucket('ceres-technical-challenge')
df = None

def load_metadata(filename='metadata.txt'):
	logging.info("Loading metadata to dataframe.")
	global df
	metadata_path = DEST + filename
	ceres_bucket.download_file(filename, DEST + filename)
	df = pd.read_csv(metadata_path, delimiter="\t")

def parse_time_range(from_time_epoch, to_time_epoch):
	# if time range is invalid, return results for all times
	logging.info("Parsing time range from arguments: {}, {}".format(from_time_epoch, to_time_epoch))
	try:
		from_time_epoch = float(from_time_epoch)
		to_time_epoch = float(to_time_epoch)
	except ValueError:
		logging.error(
			"Invalid time range: from_time_epoch:{}, to_time_epch:{}".format(from_time_epoch, to_time_epoch))
		return MIN_EPOCH, MAX_EPOCH

	# validate
	if from_time_epoch < MIN_EPOCH:
		from_time_epoch = MIN_EPOCH
	if to_time_epoch > MAX_EPOCH:
		to_time_epoch = MAX_EPOCH

	return from_time_epoch, to_time_epoch

class ImageMeta(Resource):
	def get(self, img_name):
		"""Returns metadata for a given image file

    Parameters
    ----------
    img_name: string
    	Filename of image

    Returns
    -------
    metadata : json
       Metadata of image
    """
		logging.info("Processing request for image metadata for {}".format(img_name))
		if df is None:
			load_metadata()

		# find metadata that matches image name
		image_match = df.loc[df['imageNames'] == img_name]
		formated_match = image_match.to_dict('records')
		metadata = {}
		if len(formated_match) > 0:
			metadata = formated_match[0]
			# TODO parse metadata keys to remove special characters
		return metadata

class Image(Resource):

	def get(self, img_name):
		"""Download image from S3

    Parameters
    ----------
    image_name: string
    	Filename of image to download
    """
		logging.info("Processing request or image download of filename: {}".format(img_name))
		try:
			ceres_bucket.download_file(img_name, DEST + img_name)
		except botocore.exceptions.ClientError:
			abort(404, message="Image {} doesn't exist".format(img_name))
		return '', 204

class ImageList(Resource):

	def get(self):
		"""Fetch a list of image file names from S3.

    Parameters
    ----------
    from_time_epoch, to_time_epoch : float
    	Images returned should fall within this time range. Passed in as request arguments.

    Returns
    -------
    img_list : json
       List of images
    """
		args = parser.parse_args()
		from_time_epoch = args['from_time_epoch']
		to_time_epoch = args['to_time_epoch']
		logging.info("Processing request for images from {} to {}".format(from_time_epoch, to_time_epoch))
		from_time_epoch, to_time_epoch = parse_time_range(from_time_epoch, to_time_epoch)
		from_time = datetime.fromtimestamp(from_time_epoch).replace(tzinfo=utc)
		to_time = datetime.fromtimestamp(to_time_epoch).replace(tzinfo=utc)
		img_list = []
		metadata_key = ''
		for bucket_obj in ceres_bucket.objects.all():
			key = bucket_obj.key
			if '.png' in key:
				# file is an image
				# this check is not very reliable but sufficient for the data set
				if from_time <= bucket_obj.last_modified.replace(tzinfo=utc) <= to_time:
					img_list.append({'name': key})
			elif 'metadata' in key:
				# file is the metadata
				metadata_key = key

		# reload metadata
		load_metadata(metadata_key)		
		return img_list

api.add_resource(ImageList, '/images')
api.add_resource(Image, '/images/<img_name>')
api.add_resource(ImageMeta, '/images/<img_name>/metadata')


if __name__ == "__main__":
  app.run(debug=True)


