local MessagingService = game:GetService("MessagingService")
local DataStoreService = game:GetService("DataStoreService")
local HttpService = game:GetService("HttpService")
local RunService = game:GetService("RunService")
local Players = game:GetService("Players")

local FORMAT_STRING = "New DataServices require a %s! \n Stack: %s"
local TOPIC = "SessionLocks"
local STORE = DataStoreService:GetDataStore("UniversalV1")

local DataService = {UseSessionLocks = false}
DataService.__index = DataService

local sessionLocks = {}
local errorCatch = {}
local stock = {}

type Function = (any) -> any
type Dictionary = {[string]: any}

MessagingService:SubscribeAsync(TOPIC,function(message)
	local id = tonumber(message.Data)
	if sessionLocks[id] then
		MessagingService:PublishAsync(tostring(id),"true")
	end
end)

local wait = function(waitTime)
	if not waitTime then waitTime = 0 end
	local startTime = tick()
	repeat RunService.Heartbeat:Wait() until startTime+waitTime < tick()
	return tick()-startTime, os.clock()	
end

local function ConnectData(player: Player,key: string | number,transform: Function)
	local loadedData
	local success,errorMessage = pcall(function()
		loadedData = STORE:UpdateAsync(key,transform)
	end)

	if not success then
		warn("An error has occured while loading data for "..key..": "..errorMessage)		
		if player then
			errorCatch[player.UserId] = true
			player:Kick("An error occured while loading your data, please rejoin.")
		end		
	end

	return success and loadedData
end

local function DefaultTransform(latestData)
	return latestData
end

local function FindActiveSessionLock(player)
	local timeOut = 5
	local connection
	local matchFound = false
	coroutine.wrap(function()
		connection = MessagingService:SubscribeAsync(tostring(player.UserId),function(message)
			matchFound = true
		end)
	end)()
	MessagingService:PublishAsync(TOPIC,tostring(player.UserId))
	repeat
		local elapsed = wait()
		timeOut -= elapsed
	until timeOut < 0 or matchFound
	connection:Disconnect()

	return matchFound
end

function DataService.new(uuid, key: string | number, default: Dictionary)
	assert(uuid,string.format(FORMAT_STRING,"UUID",debug.traceback(2)))
	assert(key,string.format(FORMAT_STRING,"key",debug.traceback(2)))
	local self = setmetatable({
		UUID = uuid;
		Data = {};
		Key = tostring(key)..tostring(uuid);
		Default = default;

		_saved = Instance.new("BindableEvent")
	},DataService)

	if self.UUID.ClassName then
		self.UUID = self.UUID.UserId
		self.Key = tostring(key)..tostring(self.UUID)
		self.Player = Players:GetPlayerByUserId(self.UUID)
	end

	self.OnSave = self._saved.Event

	self.Data = self:Load()
	stock[uuid] = self

	return self	
end

function DataService:Save()	
	if errorCatch[self.UUID] then
		return
	end
	
	local function Transform()
		return HttpService:JSONEncode(self.Data)
	end
	ConnectData(self.Player,self.Key,Transform)
	
	if self.Player and DataService.UseSessionLocks then
		coroutine.wrap(function()
			sessionLocks[self.UUID] = true
			wait(10)
			sessionLocks[self.UUID] = nil
		end)()
	end
	
	self._saved:Fire()
end

function DataService:Load()
	if self.Player then
		if sessionLocks[self.UUID] then
			errorCatch[self.UUID] = true
			self.Player:Kick("Your data is currently session locked, please rejoin in a few seconds.")
			return	
		end

		if FindActiveSessionLock(self.Player) then
			errorCatch[self.UUID] = true
			self.Player:Kick("Your data is currently session locked, please rejoin in a few seconds.")
			return
		end
	end

	self.Data = ConnectData(self.Player,self.Key,DefaultTransform)
	
	if not self.Data then
		self.Data = self.Default
	elseif typeof(self.Data) == "string" then
		self.Data = HttpService:JSONDecode(self.Data)
	end

	for name,value in pairs(self.Default) do
		if self.Data[name] == nil then
			self.Data[name] = value
		end
	end

	return self.Data
end

return DataService