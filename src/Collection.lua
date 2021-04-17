local Players = game:GetService("Players")

local Promise = require(script.Parent.Parent.Promise)
local t = require(script.Parent.Parent.t)
local Document = require(script.Parent.Document)
local stackSkipAssert = require(script.Parent.stackSkipAssert).stackSkipAssert
local getTime = require(script.Parent.getTime).getTime

local DOCUMENT_COOLDOWN = 7

local Collection = {}
Collection.__index = Collection

function Collection.new(name, options)
	options = options or {}

	stackSkipAssert(options.schema ~= nil, "You must provide a schema in options")

	local runSchema = t.strictInterface(options.schema)

	local defaultDataOk, defaultDataError = runSchema(options.defaultData)

	if not defaultDataOk then
		error(("The default data you provided for collection %q does not pass your schema requirements.\n\n%s\n"):format(
			name,
			defaultDataError
		), 2)
	end

	local self = setmetatable({
		name = name;
		schema = options.schema;
		runSchema = runSchema;
		defaultData = options.defaultData;
		_migrations = options.migrations or {};
		_activeDocuments = {};
		_justClosedDocuments = {};
		_activePlayers = {};
	}, Collection)

	game:BindToClose(function()
		local promises = {}
		for documentName, document in pairs(self._activeDocuments) do
			if document:isClosed() then
				promises[documentName] = Promise.new(function(resolve)
					while self._activeDocuments[documentName] ~= nil do
						Promise.delay(0):await()
					end
					resolve()
				end)
			else
				promises[documentName] = document:close()
			end
		end
		Promise.allSettled(promises):await()
	end)

	return self
end

function Collection:getDocument(name)
	name = tostring(name)

	local delayPromise = Promise.resolve()
	if self._justClosedDocuments[name] then
		local waitTime = DOCUMENT_COOLDOWN - (getTime() - self._justClosedDocuments[name])

		if waitTime > 0 then
			warn(("Document %q in %q was recently closed. Your getDocument call will be delayed by %.1f seconds."):format(
				name,
				self.name,
				waitTime
			))
			delayPromise = Promise.delay(waitTime)
		end
	end

	return delayPromise:andThen(function()
		if self._activeDocuments[name] == nil then
			self._activeDocuments[name] = Document.new(self, name)
		end

		local promise = self._activeDocuments[name]:readyPromise()

		promise:catch(function()
			self._activeDocuments[name] = nil
		end)

		return promise
	end)
end

function Collection:_connectPlayerRemoving()
	if self._listeningPlayerRemoving then
		return
	end

	self._listeningPlayerRemoving = true
	Players.PlayerRemoving:Connect(function(player)
		if not self._activePlayers[player] then
			return
		end

		local name = "player-" .. player.UserId
		local document = self:getDocument(name)
		if not document:isClosed() then
			document:close()
		end

		self._activePlayers[player] = nil
	end)
end

function Collection:getDocumentForPlayer(player)
	stackSkipAssert(Players:FindFirstChild(player), "Player not in-game")

	self:_connectPlayerRemoving()

	local name = "player-" .. player.UserId
	local documentPromise = self:getDocument(name)

	if not self._activePlayers[player] then
		self._activePlayers[player] = true
	end

	return documentPromise
end

function Collection:getLatestMigrationVersion()
	return #self._migrations
end

function Collection:validateData(data)
	return self.runSchema(data)
end

function Collection:keyExists(key)
	return self.schema[key] ~= nil
end

function Collection:validateKey(key, value)
	if self:keyExists(key) then
		return self.schema[key](value)
	else
		return false, ("Key %q is not present in %q's schema."):format(key, self.name)
	end
end

function Collection:_removeDocument(name)
	self._justClosedDocuments[name] = getTime()
	self._activeDocuments[name] = nil

	Promise.delay(DOCUMENT_COOLDOWN):andThen(function()
		self._justClosedDocuments[name] = nil
	end)
end

return Collection