const LCERROR = '\u001b[31m'; //red
const LCSUCCESS = '\u001b[32m'; //green
const LCWARN = '\u001b[33m'; //yellow
const LCINPUT = `\u001b[34m`; //blue
const LCINFO = '\u001b[36m'; //cyan
const RESET = '\u001b[0m'; //reset

export const logger = class {
  static error(message, ...optionalParams) { console.log(LCERROR, message, RESET, ...optionalParams) }
  static warn(message, ...optionalParams) { console.log(LCWARN, message, RESET, ...optionalParams) }
  static info(message, ...optionalParams) { console.log(LCINFO, message, RESET, ...optionalParams) }
  static success(message, ...optionalParams) { console.log(LCSUCCESS, message, RESET, ...optionalParams) }
  static input(message, ...optionalParams) { console.log(LCINPUT, message, RESET, ...optionalParams) }
}
