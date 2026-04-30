import boto3

client = boto3.client('events')

with open('datos.txt', 'r') as f:
    next(f)
    for line in f:
        estado, temp = line.strip().split(',')
        temp = int(temp)

        if temp > 40:
            response = client.put_events(
                Entries=[
                    {
                        'Source': 'custom.pipeline',
                        'DetailType': 'temperatura_alta',
                        'Detail': f'{{"estado": "{estado}", "temp": {temp}}}'
                    }
                ]
            )
            print(f"Evento enviado para {estado}")
