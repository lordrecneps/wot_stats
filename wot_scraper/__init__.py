from flask import Flask
app = Flask(__name__)

from dpgwhores import dpgw

if __name__ == '__main__':
	app.run()