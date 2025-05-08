import os
import torch.nn as nn
import torch.optim as optim
# from utils.Utils import *
from TreeConvolution import tcnn
import torch
from collections import OrderedDict

# os.environ["CUDA_VISIBLE_DEVICES"] = "1,2,3"
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

#init value network
class Net:
    def __init__(self, path='./Models/now.pth', learning_rate=0.0008, steps_per_epoch=100, epoch=120, max_lr=0.001):
        self.path = path
        self.model = nn.Sequential(
            tcnn.QueryEncoder(318),
            tcnn.BinaryTreeConv(109, 512),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.BinaryTreeConv(512, 256),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.BinaryTreeConv(256, 128),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.DynamicPooling(),
            # tcnn.RegNorm(128)
            PolicyHead(128)
        )
        def init_weights(m):
            if type(m) == nn.Linear:
                m.weight.data.normal_(0, 0.001)
                m.bias.data = torch.ones(m.bias.data.size())
        self.model.apply(init_weights)

        # for i in range(len(self.model)):
        #     print(self.model[i])
        # print(list(self.model.parameters()))
        self.model = self.model.to(device)

        # self.optimizer = optim.SGD(self.model.parameters(), lr=0.00001, momentum=0.9)
        self.optimizer = optim.AdamW(self.model.parameters(), lr=learning_rate)
        self.scheduler = torch.optim.lr_scheduler.OneCycleLR(self.optimizer, max_lr=max_lr, steps_per_epoch=steps_per_epoch,
                                                        epochs=epoch)


        if os.path.exists(self.path):
            print('Model Checkpoint exists!!')
            checkpoint = torch.load(self.path)['model']
            # new_state_dict = OrderedDict()
            # for k, v in checkpoint.items():
            #     name = k[7:]  # remove `module.`，表面从第7个key值字符取到最后一个字符，正好去掉了module.
            #     new_state_dict[name] = v

            # model_dict = self.model.state_dict()
            # state_dict = {k: v for k, v in checkpoint['model'].items() if k in model_dict.keys()}
            # self.model.load_state_dict(new_state_dict)
            self.model.load_state_dict(checkpoint)
            # self.optimizer.load_state_dict(checkpoint['optimizer'])



    def save(self):
        state = {'model': self.model.state_dict(), 'optimizer': self.optimizer.state_dict()}
        torch.save(state, self.path)

class PolicyHead(nn.Module):
    def __init__(self, input_size):
        super().__init__()
        self.hidden1 = nn.Linear(input_size, 128)
        self.hidden2 = nn.Linear(128, 64)
        self.hidden3 = nn.Linear(64, 32)
        self.hidden4 = nn.Linear(32, 168)  # Matches Pi_net's output dimension
        self.activation = nn.ReLU()
        self.drop_layer = nn.Dropout(p=0.5)

    def forward(self, x):
        x = self.drop_layer(x)
        x = self.activation(self.hidden1(x))
        x = self.activation(self.hidden2(x))
        x = self.drop_layer(x)
        x = self.activation(self.hidden3(x))
        x = self.hidden4(x)
        return x

class Net_latency(nn.Module):
    def __init__(self, path='./Models/now.pth', learning_rate=0.0008, steps_per_epoch=100, epoch=120, max_lr=0.001):
        super(Net_latency, self).__init__()
        self.path = path

        self.layer0 = tcnn.QueryEncoder(318)
        self.layer1 = tcnn.BinaryTreeConv(109, 512)
        self.layer2 = tcnn.TreeLayerNorm()
        self.layer3 = tcnn.TreeActivation(nn.ReLU())
        self.layer4 = tcnn.BinaryTreeConv(512, 256)
        self.layer5 = tcnn.TreeLayerNorm()
        self.layer6 = tcnn.TreeActivation(nn.ReLU())
        self.layer7 = tcnn.BinaryTreeConv(256, 128)
        self.layer8 = tcnn.TreeLayerNorm()
        self.layer9 = tcnn.TreeActivation(nn.ReLU())
        self.layer10 = tcnn.DynamicPooling()
        # tcnn.RegNorm(128)
        self.cost_layer = tcnn.RegNorm(128)
        self.latency_layer = tcnn.RegNorm(128)

        def init_weights(m):
            if type(m) == nn.Linear:
                m.weight.data.normal_(0, 0.001)
                m.bias.data = torch.ones(m.bias.data.size())

        self.apply(init_weights)

        # for i in range(len(self.model)):
        #     print(self.model[i])
        # print(list(self.model.parameters()))
        if torch.cuda.is_available():
            self.cuda()

        # self.optimizer = optim.SGD(self.model.parameters(), lr=0.00001, momentum=0.9)
        self.optimizer = optim.AdamW(self.parameters(), lr=learning_rate)
        self.scheduler = torch.optim.lr_scheduler.OneCycleLR(self.optimizer, max_lr=max_lr,
                                                             steps_per_epoch=steps_per_epoch,
                                                             epochs=epoch)

        if os.path.exists(self.path):
            print('Model Checkpoint exists!!')
            checkpoint = torch.load(self.path)['model']
            self.load_state_dict(checkpoint)
            # self.model.load_state_dict(checkpoint)
            # self.optimizer.load_state_dict(checkpoint['optimizer'])

    def forward(self, state):
        state = self.layer0(state)
        state = self.layer1(state)
        state = self.layer2(state)
        state = self.layer3(state)
        state = self.layer4(state)
        state = self.layer5(state)
        state = self.layer6(state)
        state = self.layer7(state)
        state = self.layer8(state)
        state = self.layer9(state)
        state = self.layer10(state)
        latency = self.latency_layer(state)
        cost = self.cost_layer(state)
        return latency, cost

    def save(self):
        state = {'model': self.state_dict(), 'optimizer': self.optimizer.state_dict()}
        torch.save(state, self.path)


class Net_min:
    def __init__(self, path='./Models/now_min.pth', learning_rate=0.0008, steps_per_epoch=100, epoch=120, max_lr=0.001):
        self.path = path
        self.model = nn.Sequential(
            tcnn.QueryEncoder(318),
            tcnn.BinaryTreeConv(109, 512),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.BinaryTreeConv(512, 256),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.BinaryTreeConv(256, 128),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.DynamicPooling_Min(),
            tcnn.RegNorm(128)
        )
        def init_weights(m):
            if type(m) == nn.Linear:
                m.weight.data.normal_(0, 0.001)
                m.bias.data = torch.ones(m.bias.data.size())
        self.model.apply(init_weights)

        # for i in range(len(self.model)):
        #     print(self.model[i])
        # print(list(self.model.parameters()))
        if torch.cuda.is_available():
            self.model = self.model.cuda()

        # self.optimizer = optim.SGD(self.model.parameters(), lr=0.00001, momentum=0.9)
        self.optimizer = optim.AdamW(self.model.parameters(), lr=learning_rate)
        self.scheduler = torch.optim.lr_scheduler.OneCycleLR(self.optimizer, max_lr=max_lr, steps_per_epoch=steps_per_epoch,
                                                        epochs=epoch)

        if os.path.exists(self.path):
            checkpoint = torch.load(self.path)
            # model_dict = self.model.state_dict()
            # state_dict = {k: v for k, v in checkpoint['model'].items() if k in model_dict.keys()}
            self.model.load_state_dict(checkpoint)
            # self.model.load_state_dict(checkpoint['model'])
            # self.optimizer.load_state_dict(checkpoint['optimizer'])


    def save(self):
        state = {'model': self.model.state_dict(), 'optimizer': self.optimizer.state_dict()}
        torch.save(state, self.path)



if __name__ == '__main__':

    model = nn.Sequential(
            tcnn.QueryEncoder(318),
            tcnn.BinaryTreeConv(109, 512),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.BinaryTreeConv(512, 256),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.BinaryTreeConv(256, 128),
            tcnn.TreeLayerNorm(),
            tcnn.TreeActivation(nn.ReLU()),
            tcnn.DynamicPooling_Min(),
            tcnn.RegNorm(128)
        )
    checkpoint = torch.load('/data/hdd1/users/kmparmp/BASE/Models/now.pth', map_location=torch.device('cpu'))

    # model_dict = model.state_dict()
    # # print(model_dict.keys())
    # # print(checkpoint.keys())
    # state_dict = {k: v for k, v in checkpoint['model'].items() if k in model_dict.keys()}
    model.load_state_dict(checkpoint['model'])
    input = [ [[0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], ((1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1), ((0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1), ((0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1), ((0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), ((0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),)), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1),)), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0),)), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),))],[[0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], ((1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1), ((0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1), ((0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1), ((0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1), ((0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1), ((0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), ((0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),)), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1),)), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0),)), ((0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),)), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),)), ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),))]
    ]
    # input = [[input[0][0], ]]
    #zero = [[0] * len(input[0][0]), (tuple([0] * (21 * 2 + 3)),)]
    #print(model([zero]))

    print(model(input))
    print(model(input[0]))
    print(model(input[1]))