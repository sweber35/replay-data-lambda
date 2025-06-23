export const matchSettingsDefaults = {
    isTeams: false,
    isPal: false,
    isFrozenStadium: false,
    platform: 'dolphin',
    consoleNickname: 'slippi',
    timerType: 'counting down',
    characterUiPlacesCount: 0, // what is this and does it matter
    gameType: 'stock',
    friendlyFireOn: true,
    isBreakTheTargetsOrTitleDemo: false,
    isClassicOrAdventureMode: false,
    isHomeRunContestOrEventMatch: false,
    isSingleButtonMode: false,
    timerCountsDuringPause: false,
    bombRain: false,
    itemSpawnRate: 'off',
    selfDestructScoreValue: 0,
    damageRatio: 1
}

export const playerSettingsDefaults = {
    startStocks: 4,
    costumeIndex: 1, // TODO: will these have to be computed eventually since each player needs a different index
    teamShade: 1,
    handicap: 0,
    teamId: 1,
    staminaMode: false,
    silentCharacter: false,
    lowGravity: false,
    invisible: false,
    blackStockIcon: false,
    metal: false,
    startGameOnWarpPlatform: false,
    rumbleEnabled: false,
    cpuLevel: 1,
    offenseRatio: 1,
    defenseRatio: 1,
    modelScale: 1,
    controllerFix: "UCF",
    internalCharacterIds: [] // TODO: what is going on with this value
}

export const playerStateDefaults = {
    isReflectActive: false,
    isFastfalling: false,
    isShieldActive: false,
    isHittingShield: false,
    isPowershieldActive: false,
    isOffscreen: false,
}

export const itemStateDefaults = {
    damageTaken: 0,
    expirationTimer: 0,
}

export const gameEndingDefaults = {
    gameEndMethod: 'GAME!',
    oldGameEndMethod: 'resolved',
    quitInitiator: 0
}
