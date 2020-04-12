import {DynamoDB, Request, AWSError} from "aws-sdk";
import aws from "../aws";
import CustomError from "../Error";
import {Schema, SchemaDefinition} from "../Schema";
import utils from "../utils";
import Internal from "../Internal";

export interface ModelWaitForActiveSettings {
	enabled: boolean;
	check: {timeout: number; frequency: number};
}
export interface ModelExpiresSettings {
	ttl: number;
	attribute: string;
	items?: {
		returnExpired: boolean;
	};
}
export interface ModelOptions {
	create: boolean;
	throughput: number | {read: number; write: number};
	prefix: string;
	suffix: string;
	waitForActive: ModelWaitForActiveSettings;
	update: boolean;
	expires?: number | ModelExpiresSettings;
}
type ModelOptionsOptional = Partial<ModelOptions>;

type RType<T> = T&{
    options: ModelOptions,
    tableName: string,
    schema: Schema | SchemaDefinition,
    ready: boolean,
    pendingTasks: Array<any>,
    latestTableDetails: any,
    pendingTaskPromise: () => Promise<void>,
};

// Defaults
export const defaults: ModelOptions = {
	"create": true,
	"throughput": {
		"read": 5,
		"write": 5
	},
	"prefix": "",
	"suffix": "",
	"waitForActive": {
		"enabled": true,
		"check": {
			"timeout": 180000,
			"frequency": 1000
		}
	},
	"update": false,
	"expires": undefined
	// "streamOptions": {
	// 	"enabled": false,
	// 	"type": undefined
	// },
	// "serverSideEncryption": false,
	// "defaultReturnValues": "ALL_NEW",
};

export const construct = (schema: Schema | SchemaDefinition, options: ModelOptionsOptional = {}) => <T extends { name: string, defaults: {} }>(target: T): RType<T> => {
    const decorated = target as RType<T>;
    (decorated as RType<T>).options = (utils.combine_objects(options, target.defaults, defaults) as ModelOptions);
    (decorated as RType<T>).tableName = `${decorated.options.prefix}${target.name}${decorated.options.suffix}`;

    if (!schema) {
        throw new CustomError.MissingSchemaError(`Schema hasn't been registered for model "${target.name}".\nUse "new dynamoose.Model(name, schema)"`);
    } else if (!(schema instanceof Schema)) {
        schema = new Schema(schema);
    }
    if (options.expires) {
        if (typeof options.expires === "number") {
            options.expires = {
                "attribute": "ttl",
                "ttl": options.expires
            };
        }
        options.expires = utils.combine_objects((options.expires as any), {"attribute": "ttl"});

        schema.schemaObject[(options.expires as any).attribute] = {
            "type": {
                "value": Date,
                "settings": {
                    "storage": "seconds"
                }
            },
            "default": (): Date => new Date(Date.now() + (options.expires as any).ttl)
        };
        schema[Internal.Schema.internalCache].attributes = undefined;
    }
    decorated.schema = schema;

    // Setup flow
    decorated.ready = false; // Represents if model is ready to be used for actions such as "get", "put", etc. This property being true does not guarantee anything on the DynamoDB server. It only guarantees that Dynamoose has finished the initalization steps required to allow the model to function as expected on the client side.
    decorated.pendingTasks = []; // Represents an array of promise resolver functions to be called when Model.ready gets set to true (at the end of the setup flow)
    decorated.latestTableDetails = null; // Stores the latest result from `describeTable` for the given table
    decorated.pendingTaskPromise = () => { // Returns a promise that will be resolved after the Model is ready. This is used in all Model operations (Model.get, Document.save) to `await` at the beginning before running the AWS SDK method to ensure the Model is setup before running actions on it.
        return decorated.ready ? Promise.resolve() : new Promise((resolve) => {
            decorated.pendingTasks.push(resolve);
        });
    };
    const setupFlow = []; // An array of setup actions to be run in order
    // Create table
    if (decorated.options.create) {
        setupFlow.push(() => createTable(target));
    }
    // Wait for Active
    if ((decorated.options.waitForActive || {}).enabled) {
        setupFlow.push(() => waitForActive(target));
    }
    // Update Time To Live
    if ((decorated.options.create || decorated.options.update) && options.expires) {
        setupFlow.push(() => updateTimeToLive(target));
    }
    // Update
    if (decorated.options.update) {
        setupFlow.push(() => updateTable(target));
    }

    // Run setup flow
    const setupFlowPromise = setupFlow.reduce((existingFlow, flow) => {
        return existingFlow.then(() => flow()).then((flow) => {
            const flowItem = typeof flow === "function" ? flow : flow.promise;
            return flowItem instanceof Promise ? flowItem : flowItem.bind(flow)();
        });
    }, Promise.resolve());
    setupFlowPromise.then(() => decorated.ready = true).then(() => {decorated.pendingTasks.forEach((task) => task()); decorated.pendingTasks = [];});

    const ModelStore = require("./ModelStore");
    ModelStore(target);

    return decorated;
}

// Utility functions
async function getTableDetails<T extends any>(model: T, settings: {allowError?: boolean; forceRefresh?: boolean} = {}): Promise<DynamoDB.DescribeTableOutput> {
	const func = async (): Promise<void> => {
		const tableDetails: DynamoDB.DescribeTableOutput = await aws.ddb().describeTable({"TableName": model.name}).promise();
		model.latestTableDetails = tableDetails; // eslint-disable-line require-atomic-updates
	};
	if (settings.forceRefresh || !model.latestTableDetails) {
		if (settings.allowError) {
			try {
				await func();
			} catch (e) {} // eslint-disable-line no-empty
		} else {
			await func();
		}
	}

	return model.latestTableDetails;
}
async function createTableRequest<T extends any>(model: T): Promise<DynamoDB.CreateTableInput> {
	return {
		"TableName": model.name,
		...utils.dynamoose.get_provisioned_throughput(model.options),
		...await model.schema.getCreateTableAttributeParams(model)
	};
}
async function createTable<T extends any>(model: T): Promise<Request<DynamoDB.CreateTableOutput, AWSError> | {promise: () => Promise<void>}> {
	if ((((await getTableDetails(model, {"allowError": true})) || {}).Table || {}).TableStatus === "ACTIVE") {
		return {"promise": (): Promise<void> => Promise.resolve()};
	}

	return aws.ddb().createTable(await createTableRequest(model));
}
function updateTimeToLive<T extends any>(model: T): {promise: () => Promise<void>} {
	return {
		"promise": async (): Promise<void> => {
			let ttlDetails;

			async function updateDetails(): Promise<void> {
				ttlDetails = await aws.ddb().describeTimeToLive({
					"TableName": model.name
				}).promise();
			}
			await updateDetails();

			function updateTTL(): Request<DynamoDB.UpdateTimeToLiveOutput, AWSError> {
				return aws.ddb().updateTimeToLive({
					"TableName": model.name,
					"TimeToLiveSpecification": {
						"AttributeName": (model.options.expires as any).attribute,
						"Enabled": true
					}
				});
			}

			switch (ttlDetails.TimeToLiveDescription.TimeToLiveStatus) {
			case "DISABLING":
				while (ttlDetails.TimeToLiveDescription.TimeToLiveStatus === "DISABLING") {
					await utils.timeout(1000);
					await updateDetails();
				}
				// fallthrough
			case "DISABLED":
				await updateTTL();
				break;
			default:
				break;
			}
		}
	};
}
function waitForActive<T extends any>(model: T) {
	return (): Promise<void> => new Promise((resolve, reject) => {
		const start = Date.now();
		async function check(count): Promise<void> {
			try {
				// Normally we'd want to do `dynamodb.waitFor` here, but since it doesn't work with tables that are being updated we can't use it in this case
				if ((await getTableDetails(model, {"forceRefresh": count > 0})).Table.TableStatus === "ACTIVE") {
					return resolve();
				}
			} catch (e) {
				return reject(e);
			}

			if (count > 0) {
				model.options.waitForActive.check.frequency === 0 ? await utils.set_immediate_promise() : await utils.timeout(model.options.waitForActive.check.frequency);
			}
			if ((Date.now() - start) >= model.options.waitForActive.check.timeout) {
				return reject(new CustomError.WaitForActiveTimeout(`Wait for active timed out after ${Date.now() - start} milliseconds.`));
			} else {
				check(++count);
			}
		}
		check(0);
	});
}
async function updateTable<T extends any>(model: T): Promise<Request<DynamoDB.UpdateTableOutput, AWSError> | {promise: () => Promise<void>}> {
	const currentThroughput = (await getTableDetails(model)).Table;
	const expectedThroughput: any = utils.dynamoose.get_provisioned_throughput(model.options);
	if ((expectedThroughput.BillingMode === currentThroughput.BillingModeSummary?.BillingMode && expectedThroughput.BillingMode) || ((currentThroughput.ProvisionedThroughput || {}).ReadCapacityUnits === (expectedThroughput.ProvisionedThroughput || {}).ReadCapacityUnits && currentThroughput.ProvisionedThroughput.WriteCapacityUnits === expectedThroughput.ProvisionedThroughput.WriteCapacityUnits)) {
	// if ((expectedThroughput.BillingMode === currentThroughput.BillingModeSummary.BillingMode && expectedThroughput.BillingMode) || ((currentThroughput.ProvisionedThroughput || {}).ReadCapacityUnits === (expectedThroughput.ProvisionedThroughput || {}).ReadCapacityUnits && currentThroughput.ProvisionedThroughput.WriteCapacityUnits === expectedThroughput.ProvisionedThroughput.WriteCapacityUnits)) {
		return {"promise": (): Promise<void> => Promise.resolve()};
	}

	const object: DynamoDB.UpdateTableInput = {
		"TableName": model.name,
		...expectedThroughput
	};
	return aws.ddb().updateTable(object);
}