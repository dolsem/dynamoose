import CustomError from "./Error";
import {Schema, SchemaDefinition} from "./Schema";
import {Document as DocumentCarrier} from "./Document";
import utils from "./utils";
import aws from "./aws";
import Internal from "./Internal";
import Condition from "./Condition";
import {Scan, Query, ConditionInitalizer} from "./DocumentRetriever";
import {CallbackType} from "./General";
import {ModelOptions, construct, defaults} from "./decorators/construct";

import {DynamoDB, Request, AWSError} from "aws-sdk";





type InputKey = string | {[attribute: string]: string};
function convertObjectToKey(this: Model, key: InputKey): {[key: string]: string} {
	let keyObject: {[key: string]: string};
	const hashKey = this.schema.getHashKey();
	if (typeof key === "object") {
		const rangeKey = this.schema.getRangeKey();
		keyObject = {
			[hashKey]: key[hashKey]
		};
		if (rangeKey && key[rangeKey]) {
			keyObject[rangeKey] = key[rangeKey];
		}
	} else {
		keyObject = {
			[hashKey]: key
		};
	}

	return keyObject;
}


// Model represents one DynamoDB table
export class Model extends DocumentCarrier {
	static options: ModelOptions;
	static schema: Schema;
	private static ready: boolean;
	private static pendingTasks: any[];
	static latestTableDetails: DynamoDB.DescribeTableOutput;
	static pendingTaskPromise: () => Promise<void>;
	static defaults: ModelOptions;
	static Document: typeof DocumentCarrier;
	static scan: (this: Model, object?: ConditionInitalizer) => Scan;
	static query: (this: Model, object?: ConditionInitalizer) => Query;
	static get: (this: Model, key: InputKey, settings?: ModelGetSettings, callback?: CallbackType<DocumentCarrier | DynamoDB.GetItemInput, AWSError>) => void | DynamoDB.GetItemInput | Promise<DocumentCarrier>;
	static delete: (this: Model, key: InputKey, settings?: ModelDeleteSettings, callback?: CallbackType<DynamoDB.DeleteItemInput, AWSError>) => void | DynamoDB.DeleteItemInput | Promise<void>;
	static batchDelete: (this: Model, keys: InputKey[], settings?: ModelBatchDeleteSettings, callback?: any) => void | DynamoDB.BatchWriteItemInput | Promise<any>;
	static create: (this: Model, document: any, settings?: {}, callback?: any) => void | Promise<any>;
	static batchPut: (this: Model, items: any, settings?: {}, callbac?: any) => void | Promise<any>;
	static update: (this: Model, keyObj: any, updateObj: any, settings?: ModelUpdateSettings, callback?: any) => void | Promise<any>;
	static batchGet: (this: Model, keys: InputKey[], settings?: ModelBatchGetSettings, callback?: any) => void | DynamoDB.BatchGetItemInput | Promise<any>;
	static methods: { document: { set: (name: string, fn: any) => void; delete: (name: string) => void }; set: (name: string, fn: any) => void; delete: (name: string) => void };
}

Model.defaults = defaults;


interface ModelGetSettings {
	return: "document" | "request";
}
Model.get = function (this: Model, key: InputKey, settings: ModelGetSettings = {"return": "document"}, callback): void | DynamoDB.GetItemInput | Promise<DocumentCarrier> {
	if (typeof settings === "function") {
		callback = settings;
		settings = {"return": "document"};
	}

	const documentify = (document: DynamoDB.AttributeMap): Promise<DocumentCarrier> => (new this.Document((document as any), {"fromDynamo": true})).conformToSchema({"customTypesDynamo": true, "checkExpiredItem": true, "saveUnknown": true, "modifiers": ["get"], "type": "fromDynamo"});

	const getItemParams = {
		"Key": this.Document.objectToDynamo(convertObjectToKey.bind(this)(key)),
		"TableName": this.name
	};
	if (settings.return === "request") {
		if (callback) {
			callback(null, getItemParams);
			return;
		} else {
			return getItemParams;
		}
	}
	const promise = this.pendingTaskPromise().then(() => aws.ddb().getItem(getItemParams).promise());

	if (callback) {
		promise.then((response) => response.Item ? documentify(response.Item) : undefined).then((response) => callback(null, response)).catch((error) => callback(error));
	} else {
		return (async (): Promise<any> => {
			const response = await promise;
			return response.Item ? await documentify(response.Item) : undefined;
		})();
	}
};
interface ModelBatchGetSettings {
	return: "documents" | "request";
}
Model.batchGet = function (this: Model, keys: InputKey[], settings: ModelBatchGetSettings = {"return": "documents"}, callback): void | DynamoDB.BatchGetItemInput | Promise<any> {
	if (typeof settings === "function") {
		callback = settings;
		settings = {"return": "documents"};
	}

	const keyObjects = keys.map((key) => convertObjectToKey.bind(this)(key));

	const documentify = (document): Promise<any> => (new this.Document(document, {"fromDynamo": true})).conformToSchema({"customTypesDynamo": true, "checkExpiredItem": true, "saveUnknown": true, "modifiers": ["get"], "type": "fromDynamo"});
	const prepareResponse = async (response): Promise<any> => {
		const tmpResult = await Promise.all(response.Responses[this.name].map((item) => documentify(item)));
		const unprocessedArray = response.UnprocessedKeys[this.name] ? response.UnprocessedKeys[this.name].Keys : [];
		const tmpResultUnprocessed = await Promise.all(unprocessedArray.map((item) => this.Document.fromDynamo(item)));
		const startArray: any = [];
		startArray.unprocessedKeys = [];
		return keyObjects.reduce((result, key) => {
			const keyProperties = Object.keys(key);
			let item = tmpResult.find((item) => keyProperties.every((keyProperty) => item[keyProperty] === key[keyProperty]));
			if (item) {
				result.push(item);
			} else {
				item = tmpResultUnprocessed.find((item) => keyProperties.every((keyProperty) => item[keyProperty] === key[keyProperty]));
				if (item) {
					result.unprocessedKeys.push(item);
				}
			}
			return result;
		}, startArray);
	};

	const params = {
		"RequestItems": {
			[this.name]: {
				"Keys": keyObjects.map((key) => this.Document.objectToDynamo(key))
			}
		}
	};
	if (settings.return === "request") {
		if (callback) {
			callback(null, params);
			return;
		} else {
			return params;
		}
	}
	const promise = this.pendingTaskPromise().then(() => aws.ddb().batchGetItem(params).promise());

	if (callback) {
		promise.then((response) => prepareResponse(response)).then((response) => callback(null, response)).catch((error) => callback(error));
	} else {
		return (async (): Promise<any> => {
			const response = await promise;
			return prepareResponse(response);
		})();
	}
};

Model.create = function (this: Model, document, settings = {}, callback): void | Promise<any> {
	if (typeof settings === "function" && !callback) {
		callback = settings;
		settings = {};
	}

	return (new this.Document(document)).save({"overwrite": false, ...settings}, callback);
};
interface ModelBatchPutSettings {
	return: "response" | "request";
}
Model.batchPut = function (this: Model, items, settings: ModelBatchPutSettings = {"return": "response"}, callback): void | Promise<any> {
	if (typeof settings === "function") {
		callback = settings;
		settings = {"return": "response"};
	}

	const prepareResponse = async (response: DynamoDB.BatchWriteItemOutput): Promise<{unprocessedItems: any[]}> => {
		const unprocessedArray = response.UnprocessedItems && response.UnprocessedItems[this.name] ? response.UnprocessedItems[this.name] : [];
		const tmpResultUnprocessed = await Promise.all(unprocessedArray.map((item) => this.Document.fromDynamo(item.PutRequest.Item)));
		return items.reduce((result, document) => {
			const item = tmpResultUnprocessed.find((item) => Object.keys(document).every((keyProperty) => item[keyProperty] === document[keyProperty]));
			if (item) {
				result.unprocessedItems.push(item);
			}
			return result;
		}, {"unprocessedItems": []});
	};

	const paramsPromise: Promise<DynamoDB.BatchWriteItemInput> = (async (): Promise<DynamoDB.BatchWriteItemInput> => ({
		"RequestItems": {
			[this.name]: await Promise.all(items.map(async (item) => ({
				"PutRequest": {
					"Item": await (new this.Document(item)).toDynamo({"defaults": true, "validate": true, "required": true, "enum": true, "forceDefault": true, "saveUnknown": true, "customTypesDynamo": true, "updateTimestamps": true, "modifiers": ["set"]})
				}
			})))
		}
	}))();
	if (settings.return === "request") {
		if (callback) {
			paramsPromise.then((result) => callback(null, result));
			return;
		} else {
			return paramsPromise;
		}
	}
	const promise = this.pendingTaskPromise().then(() => paramsPromise).then((params) => aws.ddb().batchWriteItem(params).promise());

	if (callback) {
		promise.then((response) => prepareResponse(response)).then((response) => callback(null, response)).catch((error) => callback(error));
	} else {
		return (async (): Promise<{unprocessedItems: any[]}> => {
			const response = await promise;
			return prepareResponse(response);
		})();
	}
};

interface ModelUpdateSettings {
	return: "document" | "request";
	condition?: Condition;
}
Model.update = function (this: Model, keyObj, updateObj, settings: ModelUpdateSettings = {"return": "document"}, callback): void | Promise<any> {
	if (typeof updateObj === "function") {
		callback = updateObj;
		updateObj = null;
		settings = {"return": "document"};
	}
	if (typeof settings === "function") {
		callback = settings;
		settings = {"return": "document"};
	}
	if (!updateObj) {
		const hashKeyName = this.schema.getHashKey();
		updateObj = keyObj;
		keyObj = {
			[hashKeyName]: keyObj[hashKeyName]
		};
		delete updateObj[hashKeyName];
	}

	let index = 0;
	// TODO: change the line below to not be partial
	const getUpdateExpressionObject: () => Promise<any> = async () => {
		const updateTypes = [
			{"name": "$SET", "operator": " = ", "objectFromSchemaSettings": {"validate": true, "enum": true, "forceDefault": true, "required": "nested"}},
			{"name": "$ADD", "objectFromSchemaSettings": {"forceDefault": true}},
			{"name": "$REMOVE", "attributeOnly": true, "objectFromSchemaSettings": {"required": true, "defaults": true}}
		].reverse();
		const returnObject: any = await Object.keys(updateObj).reduce(async (accumulatorPromise, key) => {
			const accumulator = await accumulatorPromise;
			let value = updateObj[key];

			if (!(typeof value === "object" && updateTypes.map((a) => a.name).includes(key))) {
				value = {[key]: value};
				key = "$SET";
			}

			const valueKeys = Object.keys(value);
			for (let i = 0; i < valueKeys.length; i++) {
				let subKey = valueKeys[i];
				let subValue = value[subKey];

				let updateType = updateTypes.find((a) => a.name === key);

				const expressionKey = `#a${index}`;
				subKey = Array.isArray(value) ? subValue : subKey;

				const dynamoType = this.schema.getAttributeType(subKey, subValue, {"unknownAttributeAllowed": true});
				const attributeExists = this.schema.attributes().includes(subKey);
				const dynamooseUndefined = require("./index").undefined;
				if (!updateType.attributeOnly && subValue !== dynamooseUndefined) {
					subValue = (await this.Document.objectFromSchema({[subKey]: dynamoType === "L" && !Array.isArray(subValue) ? [subValue] : subValue}, this, ({"type": "toDynamo", "customTypesDynamo": true, "saveUnknown": true, ...updateType.objectFromSchemaSettings} as any)))[subKey];
				}

				if (subValue === dynamooseUndefined || subValue === undefined) {
					if (attributeExists) {
						updateType = updateTypes.find((a) => a.name === "$REMOVE");
					} else {
						continue;
					}
				}

				if (subValue !== dynamooseUndefined) {
					const defaultValue = await this.schema.defaultCheck(subKey, undefined, updateType.objectFromSchemaSettings);
					if (defaultValue) {
						subValue = defaultValue;
						updateType = updateTypes.find((a) => a.name === "$SET");
					}
				}

				if (updateType.objectFromSchemaSettings.required === true) {
					await this.schema.requiredCheck(subKey, undefined);
				}

				let expressionValue = updateType.attributeOnly ? "" : `:v${index}`;
				accumulator.ExpressionAttributeNames[expressionKey] = subKey;
				if (!updateType.attributeOnly) {
					accumulator.ExpressionAttributeValues[expressionValue] = subValue;
				}

				if (dynamoType === "L" && updateType.name === "$ADD") {
					expressionValue = `list_append(${expressionKey}, ${expressionValue})`;
					updateType = updateTypes.find((a) => a.name === "$SET");
				}

				const operator = updateType.operator || (updateType.attributeOnly ? "" : " ");

				accumulator.UpdateExpression[updateType.name.slice(1)].push(`${expressionKey}${operator}${expressionValue}`);

				index++;
			}

			return accumulator;
		}, Promise.resolve((async (): Promise<{ExpressionAttributeNames: any; ExpressionAttributeValues: any; UpdateExpression: any}> => {
			const obj = {
				"ExpressionAttributeNames": {},
				"ExpressionAttributeValues": {},
				"UpdateExpression": updateTypes.map((a) => a.name).reduce((accumulator, key) => {
					accumulator[key.slice(1)] = [];
					return accumulator;
				}, {})
			};

			const documentFunctionSettings = {"updateTimestamps": {"updatedAt": true}, "customTypesDynamo": true, "type": "toDynamo"};
			const defaultObjectFromSchema = await this.Document.objectFromSchema(this.Document.prepareForObjectFromSchema({}, this, (documentFunctionSettings as any)), this, (documentFunctionSettings as any));
			Object.keys(defaultObjectFromSchema).forEach((key) => {
				const value = defaultObjectFromSchema[key];
				const updateType = updateTypes.find((a) => a.name === "$SET");

				obj.ExpressionAttributeNames[`#a${index}`] = key;
				obj.ExpressionAttributeValues[`:v${index}`] = value;
				obj.UpdateExpression[updateType.name.slice(1)].push(`#a${index}${updateType.operator}:v${index}`);

				index++;
			});

			return obj;
		})()));

		await Promise.all(this.schema.attributes().map(async (attribute) => {
			const defaultValue = await this.schema.defaultCheck(attribute, undefined, {"forceDefault": true});
			if (defaultValue && !Object.values(returnObject.ExpressionAttributeNames).includes(attribute)) {
				const updateType = updateTypes.find((a) => a.name === "$SET");

				returnObject.ExpressionAttributeNames[`#a${index}`] = attribute;
				returnObject.ExpressionAttributeValues[`:v${index}`] = defaultValue;
				returnObject.UpdateExpression[updateType.name.slice(1)].push(`#a${index}${updateType.operator}:v${index}`);

				index++;
			}
		}));

		Object.values(returnObject.ExpressionAttributeNames).map((attribute: string, index) => {
			const value = Object.values(returnObject.ExpressionAttributeValues)[index];
			const valueKey = Object.keys(returnObject.ExpressionAttributeValues)[index];
			const dynamoType = this.schema.getAttributeType(attribute, (value as any), {"unknownAttributeAllowed": true});
			const attributeType = Schema.attributeTypes.findDynamoDBType(dynamoType);

			if (attributeType.toDynamo && !attributeType.isOfType(value, "fromDynamo")) {
				returnObject.ExpressionAttributeValues[valueKey] = attributeType.toDynamo(value);
			}
		});

		returnObject.ExpressionAttributeValues = this.Document.objectToDynamo(returnObject.ExpressionAttributeValues);
		returnObject.UpdateExpression = Object.keys(returnObject.UpdateExpression).reduce((accumulator, key) => {
			const value = returnObject.UpdateExpression[key];

			if (value.length > 0) {
				return `${accumulator}${accumulator.length > 0 ? " " : ""}${key} ${value.join(", ")}`;
			} else {
				return accumulator;
			}
		}, "");
		return returnObject;
	};

	const documentify = (document): Promise<any> => (new this.Document(document, {"fromDynamo": true})).conformToSchema({"customTypesDynamo": true, "checkExpiredItem": true, "type": "fromDynamo"});
	const updateItemParamsPromise: Promise<DynamoDB.UpdateItemInput> = this.pendingTaskPromise().then(async () => ({
		"Key": this.Document.objectToDynamo(keyObj),
		"ReturnValues": "ALL_NEW",
		...utils.merge_objects.main({"combineMethod": "object_combine"})((settings.condition ? settings.condition.requestObject({"index": {"start": index, "set": (i): void => {index = i;}}, "conditionString": "ConditionExpression"}) : {}), await getUpdateExpressionObject()),
		"TableName": this.name
	}));
	if (settings.return === "request") {
		if (callback) {
			updateItemParamsPromise.then((params) => callback(null, params));
			return;
		} else {
			return updateItemParamsPromise;
		}
	}
	const promise = updateItemParamsPromise.then((params) => aws.ddb().updateItem(params).promise());

	if (callback) {
		promise.then((response) => response.Attributes ? documentify(response.Attributes) : undefined).then((response) => callback(null, response)).catch((error) => callback(error));
	} else {
		return (async (): Promise<any> => {
			const response = await promise;
			return response.Attributes ? await documentify(response.Attributes) : undefined;
		})();
	}
};

interface ModelDeleteSettings {
	return: null | "request";
}
Model.delete = function (this: Model, key: InputKey, settings: ModelDeleteSettings = {"return": null}, callback): void | DynamoDB.DeleteItemInput | Promise<void> {
	if (typeof settings === "function") {
		callback = settings;
		settings = {"return": null};
	}

	const deleteItemParams: DynamoDB.DeleteItemInput = {
		"Key": this.Document.objectToDynamo(convertObjectToKey.bind(this)(key)),
		"TableName": this.name
	};
	if (settings.return === "request") {
		if (callback) {
			callback(null, deleteItemParams);
			return;
		} else {
			return deleteItemParams;
		}
	}
	const promise = this.pendingTaskPromise().then(() => aws.ddb().deleteItem(deleteItemParams).promise());

	if (callback) {
		promise.then(() => callback()).catch((error) => callback(error));
	} else {
		return (async (): Promise<void> => {
			await promise;
		})();
	}
};
interface ModelBatchDeleteSettings {
	return: "response" | "request";
}
Model.batchDelete = function (this: Model, keys: InputKey[], settings: ModelBatchDeleteSettings = {"return": "response"}, callback): void | DynamoDB.BatchWriteItemInput | Promise<any> {
	if (typeof settings === "function") {
		callback = settings;
		settings = {"return": "response"};
	}

	const keyObjects = keys.map((key) => convertObjectToKey.bind(this)(key));


	const prepareResponse = async (response): Promise<{unprocessedItems: any[]}> => {
		const unprocessedArray = response.UnprocessedItems && response.UnprocessedItems[this.name] ? response.UnprocessedItems[this.name] : [];
		const tmpResultUnprocessed = await Promise.all(unprocessedArray.map((item) => this.Document.fromDynamo(item.DeleteRequest.Key)));
		return keyObjects.reduce((result, key) => {
			const item = tmpResultUnprocessed.find((item) => Object.keys(key).every((keyProperty) => item[keyProperty] === key[keyProperty]));
			if (item) {
				result.unprocessedItems.push(item);
			}
			return result;
		}, {"unprocessedItems": []});
	};

	const params: DynamoDB.BatchWriteItemInput = {
		"RequestItems": {
			[this.name]: keyObjects.map((key) => ({
				"DeleteRequest": {
					"Key": this.Document.objectToDynamo(key)
				}
			}))
		}
	};
	if (settings.return === "request") {
		if (callback) {
			callback(null, params);
			return;
		} else {
			return params;
		}
	}
	const promise = this.pendingTaskPromise().then(() => aws.ddb().batchWriteItem(params).promise());

	if (callback) {
		promise.then((response) => prepareResponse(response)).then((response) => callback(null, response)).catch((error) => callback(error));
	} else {
		return (async (): Promise<{unprocessedItems: any[]}> => {
			const response = await promise;
			return prepareResponse(response);
		})();
	}
};

Model.scan = function (this: Model, object?: ConditionInitalizer): Scan {
	return new Scan(this, object);
};
Model.query = function (this: Model, object?: ConditionInitalizer): Query {
	return new Query(this, object);
};

// Methods
const customMethodFunctions = (type: "model" | "document"): {set: (name: string, fn: any) => void; delete: (name: string) => void} => {
	const entryPoint = (self): any => type === "document" ? self.Document.prototype : self.Document;
	return {
		"set": function (name: string, fn): void {
			const self: any = this;
			if (!entryPoint(this)[name] || (entryPoint(this)[name][Internal.General.internalProperties] && entryPoint(this)[name][Internal.General.internalProperties].type === "customMethod")) {
				entryPoint(this)[name] = function (...args): Promise<any> {
					const bindObject = type === "document" ? this : self.Document;
					const cb = typeof args[args.length - 1] === "function" ? args[args.length - 1] : undefined;
					if (cb) {
						const result = fn.bind(bindObject)(...args);
						if (result instanceof Promise) {
							result.then((result) => cb(null, result)).catch((err) => cb(err));
						}
					} else {
						return new Promise((resolve, reject) => {
							const result = fn.bind(bindObject)(...args, (err, result) => {
								if (err) {
									reject(err);
								} else {
									resolve(result);
								}
							});
							if (result instanceof Promise) {
								result.then(resolve).catch(reject);
							}
						});
					}
				};
				entryPoint(this)[name][Internal.General.internalProperties] = {"type": "customMethod"};
			}
		},
		"delete": function (name: string): void {
			if (entryPoint(this)[name] && entryPoint(this)[name][Internal.General.internalProperties] && entryPoint(this)[name][Internal.General.internalProperties].type === "customMethod") {
				entryPoint(this)[name] = undefined;
			}
		}
	};
};
Model.methods = {
	...customMethodFunctions("model"),
	"document": customMethodFunctions("document")
};


// Usage
@construct({})
class User extends Model {}
const user = new User();