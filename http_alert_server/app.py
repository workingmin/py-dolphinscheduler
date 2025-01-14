import json
from flask import Flask, current_app, jsonify, request


app = Flask(__name__)

DOLPHINSCHEDULER_ALERT_CONTENT_FIELD = "dsAlertMsg"

@app.route("/alert", methods=['POST'])
def alert():
    data = request.json
    msg = json.loads(data.get(DOLPHINSCHEDULER_ALERT_CONTENT_FIELD))
    
    alerts = []
    if type(msg) == list:
        alerts = msg
    elif type(msg) == dict:
        alerts.append(msg)
    else:
        current_app.logger.error(f"error type. msg: {msg}")
        return jsonify({"success": False})
    
    for alert in alerts:
        process_state = alert.get('processState')
        if process_state is None:
            # ingore non process alert
            break
        
        if process_state not in ["SUCCESS", "FAILURE"]:
            # ingore non-end process alert
            break
        
        s = set(['projectCode', 'projectName', 'owner',
             'processId', 'processDefinitionCode', 'processName', 'processType',
             'recovery', 'runTimes',
             'processStartTime', 'processEndTime', 'processHost'])
        if s.issubset(alert.keys()):
            current_app.logger.info(f"process end alter: {alert}")
            return jsonify({"success": True})
    
    current_app.logger.debug(f"ingore: {msg}")
    return jsonify({"success": False})

if __name__ == "__main__":
    app.run()