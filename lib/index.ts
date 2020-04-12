import {Model} from "./Model";
import {Schema} from "./Schema";
import Condition from "./Condition";
import transaction from "./Transaction";
import aws from "./aws";
import Internal from "./Internal";
import {construct} from "./decorators/construct";

export = {
	model: (name: string, schema: any) => construct(schema)(
		Object.defineProperty(class extends Model {}, 'name', {value: name})
	),
	Schema,
	Condition,
	transaction,
	aws,
	"undefined": Internal.Public.undefined
};
