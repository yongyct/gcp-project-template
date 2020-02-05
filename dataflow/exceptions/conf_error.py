class InvalidConfigError(Exception):
	"""Exception raised for invalid program configurations & arguments"""
	def __init__(self, message):
		super().__init__(message)
		self.message = message

class InvalidSchemaError(Exception):
	"""Exception raised for invalid schema file provided"""
	def __init__(self, message):
		super().__init__(message)
		self.message = message
