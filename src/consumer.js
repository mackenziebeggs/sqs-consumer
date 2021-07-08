"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __spreadArray = (this && this.__spreadArray) || function (to, from) {
    for (var i = 0, il = from.length, j = to.length; i < il; i++, j++)
        to[j] = from[i];
    return to;
};
exports.__esModule = true;
exports.Consumer = void 0;
var SQS = require("aws-sdk/clients/sqs");
var Debug = require("debug");
var events_1 = require("events");
var bind_1 = require("./bind");
var errors_1 = require("./errors");
var message = 'Hello, World!';
console.log(message);
var debug = Debug('sqs-consumer');
var requiredOptions = [
    'queueUrl',
    // only one of handleMessage / handleMessagesBatch is required
    'handleMessage|handleMessageBatch'
];
function createTimeout(duration) {
    var timeout;
    var pending = new Promise(function (_, reject) {
        timeout = setTimeout(function () {
            reject(new errors_1.TimeoutError());
        }, duration);
    });
    return [timeout, pending];
}
function assertOptions(options) {
    requiredOptions.forEach(function (option) {
        var possibilities = option.split('|');
        if (!possibilities.find(function (p) { return options[p]; })) {
            throw new Error("Missing SQS consumer option [ " + possibilities.join(' or ') + " ].");
        }
    });
    if (options.batchSize > 10 || options.batchSize < 1) {
        throw new Error('SQS batchSize option must be between 1 and 10.');
    }
    if (options.heartbeatInterval && !(options.heartbeatInterval < options.visibilityTimeout)) {
        throw new Error('heartbeatInterval must be less than visibilityTimeout.');
    }
}
function isConnectionError(err) {
    if (err instanceof errors_1.SQSError) {
        return (err.statusCode === 403 || err.code === 'CredentialsError' || err.code === 'UnknownEndpoint');
    }
    return false;
}
function toSQSError(err, message) {
    var sqsError = new errors_1.SQSError(message);
    sqsError.code = err.code;
    sqsError.statusCode = err.statusCode;
    sqsError.region = err.region;
    sqsError.retryable = err.retryable;
    sqsError.hostname = err.hostname;
    sqsError.time = err.time;
    return sqsError;
}
function hasMessages(response) {
    return response.Messages && response.Messages.length > 0;
}
var Consumer = /** @class */ (function (_super) {
    __extends(Consumer, _super);
    function Consumer(options) {
        var _this = _super.call(this) || this;
        assertOptions(options);
        _this.queueUrl = options.queueUrl;
        _this.handleMessage = options.handleMessage;
        _this.handleMessageBatch = options.handleMessageBatch;
        _this.handleMessageTimeout = options.handleMessageTimeout;
        _this.attributeNames = options.attributeNames || [];
        _this.messageAttributeNames = options.messageAttributeNames || [];
        _this.stopped = true;
        _this.batchSize = options.batchSize || 1;
        _this.visibilityTimeout = options.visibilityTimeout;
        _this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
        _this.heartbeatInterval = options.heartbeatInterval;
        _this.waitTimeSeconds = options.waitTimeSeconds || 20;
        _this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;
        _this.pollingWaitTimeMs = options.pollingWaitTimeMs || 0;
        _this.sqs = options.sqs || new SQS({
            region: options.region || process.env.AWS_REGION || 'eu-west-1'
        });
        bind_1.autoBind(_this);
        return _this;
    }
    Consumer.prototype.emit = function (event) {
        var args = [];
        for (var _i = 1; _i < arguments.length; _i++) {
            args[_i - 1] = arguments[_i];
        }
        return _super.prototype.emit.apply(this, __spreadArray([event], args));
    };
    Consumer.prototype.on = function (event, listener) {
        return _super.prototype.on.call(this, event, listener);
    };
    Consumer.prototype.once = function (event, listener) {
        return _super.prototype.once.call(this, event, listener);
    };
    Object.defineProperty(Consumer.prototype, "isRunning", {
        get: function () {
            return !this.stopped;
        },
        enumerable: false,
        configurable: true
    });
    Consumer.create = function (options) {
        return new Consumer(options);
    };
    Consumer.prototype.start = function () {
        if (this.stopped) {
            debug('Starting consumer');
            this.stopped = false;
            this.poll();
        }
    };
    Consumer.prototype.stop = function () {
        debug('Stopping consumer');
        this.stopped = true;
    };
    Consumer.prototype.handleSqsResponse = function (response) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        debug('Received SQS response');
                        debug(response);
                        if (!response) return [3 /*break*/, 6];
                        if (!hasMessages(response)) return [3 /*break*/, 5];
                        if (!this.handleMessageBatch) return [3 /*break*/, 2];
                        // prefer handling messages in batch when available
                        return [4 /*yield*/, this.processMessageBatch(response.Messages)];
                    case 1:
                        // prefer handling messages in batch when available
                        _a.sent();
                        return [3 /*break*/, 4];
                    case 2: return [4 /*yield*/, Promise.all(response.Messages.map(this.processMessage))];
                    case 3:
                        _a.sent();
                        _a.label = 4;
                    case 4:
                        this.emit('response_processed');
                        return [3 /*break*/, 6];
                    case 5:
                        this.emit('empty');
                        _a.label = 6;
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    Consumer.prototype.processMessage = function (message) {
        return __awaiter(this, void 0, void 0, function () {
            var heartbeat, err_1;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        this.emit('message_received', message);
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 4, 7, 8]);
                        if (this.heartbeatInterval) {
                            heartbeat = this.startHeartbeat(function (elapsedSeconds) { return __awaiter(_this, void 0, void 0, function () {
                                return __generator(this, function (_a) {
                                    return [2 /*return*/, this.changeVisabilityTimeout(message, elapsedSeconds + this.visibilityTimeout)];
                                });
                            }); });
                        }
                        return [4 /*yield*/, this.executeHandler(message)];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, this.deleteMessage(message)];
                    case 3:
                        _a.sent();
                        this.emit('message_processed', message);
                        return [3 /*break*/, 8];
                    case 4:
                        err_1 = _a.sent();
                        this.emitError(err_1, message);
                        if (!this.terminateVisibilityTimeout) return [3 /*break*/, 6];
                        return [4 /*yield*/, this.changeVisabilityTimeout(message, 0)];
                    case 5:
                        _a.sent();
                        _a.label = 6;
                    case 6: return [3 /*break*/, 8];
                    case 7:
                        clearInterval(heartbeat);
                        return [7 /*endfinally*/];
                    case 8: return [2 /*return*/];
                }
            });
        });
    };
    Consumer.prototype.receiveMessage = function (params) {
        return __awaiter(this, void 0, void 0, function () {
            var err_2;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        return [4 /*yield*/, this.sqs
                                .receiveMessage(params)
                                .promise()];
                    case 1: return [2 /*return*/, _a.sent()];
                    case 2:
                        err_2 = _a.sent();
                        throw toSQSError(err_2, "SQS receive message failed: " + err_2.message);
                    case 3: return [2 /*return*/];
                }
            });
        });
    };
    Consumer.prototype.deleteMessage = function (message) {
        return __awaiter(this, void 0, void 0, function () {
            var deleteParams, err_3;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        debug('Deleting message %s', message.MessageId);
                        deleteParams = {
                            QueueUrl: this.queueUrl,
                            ReceiptHandle: message.ReceiptHandle
                        };
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, this.sqs
                                .deleteMessage(deleteParams)
                                .promise()];
                    case 2:
                        _a.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        err_3 = _a.sent();
                        throw toSQSError(err_3, "SQS delete message failed: " + err_3.message);
                    case 4: return [2 /*return*/];
                }
            });
        });
    };
    Consumer.prototype.executeHandler = function (message) {
        return __awaiter(this, void 0, void 0, function () {
            var timeout, pending, err_4;
            var _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        _b.trys.push([0, 5, 6, 7]);
                        if (!this.handleMessageTimeout) return [3 /*break*/, 2];
                        _a = createTimeout(this.handleMessageTimeout), timeout = _a[0], pending = _a[1];
                        return [4 /*yield*/, Promise.race([
                                this.handleMessage(message),
                                pending
                            ])];
                    case 1:
                        _b.sent();
                        return [3 /*break*/, 4];
                    case 2: return [4 /*yield*/, this.handleMessage(message)];
                    case 3:
                        _b.sent();
                        _b.label = 4;
                    case 4: return [3 /*break*/, 7];
                    case 5:
                        err_4 = _b.sent();
                        if (err_4 instanceof errors_1.TimeoutError) {
                            err_4.message = "Message handler timed out after " + this.handleMessageTimeout + "ms: Operation timed out.";
                        }
                        else {
                            err_4.message = "Unexpected message handler failure: " + err_4.message;
                        }
                        throw err_4;
                    case 6:
                        clearTimeout(timeout);
                        return [7 /*endfinally*/];
                    case 7: return [2 /*return*/];
                }
            });
        });
    };
    Consumer.prototype.changeVisabilityTimeout = function (message, timeout) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                try {
                    return [2 /*return*/, this.sqs
                            .changeMessageVisibility({
                            QueueUrl: this.queueUrl,
                            ReceiptHandle: message.ReceiptHandle,
                            VisibilityTimeout: timeout
                        })
                            .promise()];
                }
                catch (err) {
                    this.emit('error', err, message);
                }
                return [2 /*return*/];
            });
        });
    };
    Consumer.prototype.emitError = function (err, message) {
        if (err.name === errors_1.SQSError.name) {
            this.emit('error', err, message);
        }
        else if (err instanceof errors_1.TimeoutError) {
            this.emit('timeout_error', err, message);
        }
        else {
            this.emit('processing_error', err, message);
        }
    };
    Consumer.prototype.poll = function () {
        var _this = this;
        if (this.stopped) {
            this.emit('stopped');
            return;
        }
        debug('Polling for messages');
        var receiveParams = {
            QueueUrl: this.queueUrl,
            AttributeNames: this.attributeNames,
            MessageAttributeNames: this.messageAttributeNames,
            MaxNumberOfMessages: this.batchSize,
            WaitTimeSeconds: this.waitTimeSeconds,
            VisibilityTimeout: this.visibilityTimeout
        };
        var currentPollingTimeout = this.pollingWaitTimeMs;
        this.receiveMessage(receiveParams)
            .then(this.handleSqsResponse)["catch"](function (err) {
            _this.emit('error', err);
            if (isConnectionError(err)) {
                debug('There was an authentication error. Pausing before retrying.');
                currentPollingTimeout = _this.authenticationErrorTimeout;
            }
            return;
        }).then(function () {
            setTimeout(_this.poll, currentPollingTimeout);
        })["catch"](function (err) {
            _this.emit('error', err);
        });
    };
    Consumer.prototype.processMessageBatch = function (messages) {
        return __awaiter(this, void 0, void 0, function () {
            var heartbeat, err_5;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        messages.forEach(function (message) {
                            _this.emit('message_received', message);
                        });
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 4, 7, 8]);
                        if (this.heartbeatInterval) {
                            heartbeat = this.startHeartbeat(function (elapsedSeconds) { return __awaiter(_this, void 0, void 0, function () {
                                return __generator(this, function (_a) {
                                    return [2 /*return*/, this.changeVisabilityTimeoutBatch(messages, elapsedSeconds + this.visibilityTimeout)];
                                });
                            }); });
                        }
                        return [4 /*yield*/, this.executeBatchHandler(messages)];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, this.deleteMessageBatch(messages)];
                    case 3:
                        _a.sent();
                        messages.forEach(function (message) {
                            _this.emit('message_processed', message);
                        });
                        return [3 /*break*/, 8];
                    case 4:
                        err_5 = _a.sent();
                        this.emit('error', err_5, messages);
                        if (!this.terminateVisibilityTimeout) return [3 /*break*/, 6];
                        return [4 /*yield*/, this.changeVisabilityTimeoutBatch(messages, 0)];
                    case 5:
                        _a.sent();
                        _a.label = 6;
                    case 6: return [3 /*break*/, 8];
                    case 7:
                        clearInterval(heartbeat);
                        return [7 /*endfinally*/];
                    case 8: return [2 /*return*/];
                }
            });
        });
    };
    Consumer.prototype.deleteMessageBatch = function (messages) {
        return __awaiter(this, void 0, void 0, function () {
            var deleteParams, err_6;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        debug('Deleting messages %s', messages.map(function (msg) { return msg.MessageId; }).join(' ,'));
                        deleteParams = {
                            QueueUrl: this.queueUrl,
                            Entries: messages.map(function (message) { return ({
                                Id: message.MessageId,
                                ReceiptHandle: message.ReceiptHandle
                            }); })
                        };
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, this.sqs
                                .deleteMessageBatch(deleteParams)
                                .promise()];
                    case 2:
                        _a.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        err_6 = _a.sent();
                        throw toSQSError(err_6, "SQS delete message failed: " + err_6.message);
                    case 4: return [2 /*return*/];
                }
            });
        });
    };
    Consumer.prototype.executeBatchHandler = function (messages) {
        return __awaiter(this, void 0, void 0, function () {
            var err_7;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        return [4 /*yield*/, this.handleMessageBatch(messages)];
                    case 1:
                        _a.sent();
                        return [3 /*break*/, 3];
                    case 2:
                        err_7 = _a.sent();
                        err_7.message = "Unexpected message handler failure: " + err_7.message;
                        throw err_7;
                    case 3: return [2 /*return*/];
                }
            });
        });
    };
    Consumer.prototype.changeVisabilityTimeoutBatch = function (messages, timeout) {
        return __awaiter(this, void 0, void 0, function () {
            var params;
            return __generator(this, function (_a) {
                params = {
                    QueueUrl: this.queueUrl,
                    Entries: messages.map(function (message) { return ({
                        Id: message.MessageId,
                        ReceiptHandle: message.ReceiptHandle,
                        VisibilityTimeout: timeout
                    }); })
                };
                try {
                    return [2 /*return*/, this.sqs
                            .changeMessageVisibilityBatch(params)
                            .promise()];
                }
                catch (err) {
                    this.emit('error', err, messages);
                }
                return [2 /*return*/];
            });
        });
    };
    Consumer.prototype.startHeartbeat = function (heartbeatFn) {
        var startTime = Date.now();
        return setInterval(function () {
            var elapsedSeconds = Math.ceil((Date.now() - startTime) / 1000);
            heartbeatFn(elapsedSeconds);
        }, this.heartbeatInterval * 1000);
    };
    return Consumer;
}(events_1.EventEmitter));
exports.Consumer = Consumer;
